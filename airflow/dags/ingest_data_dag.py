from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import random
import pandas as pd
import json
import requests  # for Teams webhook

from sqlalchemy import create_engine, text  # for DB insert

# 🔗 Microsoft Teams Incoming Webhook URL
TEAMS_WEBHOOK_URL = (
    "https://epitafr.webhook.office.com/webhookb2/"
    "34db60b7-b1fe-4264-834a-50ffd3ae6729@3534b3d7-316c-4bc9-9ede-605c860f49d2/"
    "IncomingWebhook/f75309d84d724e6284d2be4ec7391053/"
    "41e701e6-9cd4-4ef2-8a47-0e5b6c241d23/V2jEE-AFT87AY6Xrsj4fXDW2r0ftskKB9ccqgp_4FX1r01"
)

# Try to import GE helper; if it fails (GE not installed or file missing), use a dummy function
try:
    from ge_validation import run_ge_validation  # real Great Expectations helper
    print("✅ ge_validation imported successfully.")
except ImportError:
    print("⚠️ Could not import ge_validation. Using dummy run_ge_validation().")

    def run_ge_validation(csv_path: str):
        """
        Dummy fallback so the DAG still runs even if GE is not available.
        You can still demo the flow + Grafana panels.
        """
        return {
            "success": True,
            "total_expectations": 0,
            "successful_expectations": 0,
            "success_ratio": 1.0,
            "criticality": "LOW",
            "report_url": None,
            "validated_file": csv_path,
        }


RAW_DATA_PATH = "/opt/airflow/raw-data"
GOOD_DATA_PATH = "/opt/airflow/good-data"
BAD_DATA_PATH = "/opt/airflow/bad-data"
REPORTS_PATH = "/opt/airflow/reports"

os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(GOOD_DATA_PATH, exist_ok=True)
os.makedirs(BAD_DATA_PATH, exist_ok=True)
os.makedirs(REPORTS_PATH, exist_ok=True)


def read_data(**context):
    print("🚀 [read_data] Listing files in:", RAW_DATA_PATH)
    files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".csv")]
    print("📂 [read_data] Found files:", files)

    if not files:
        # For ingestion DAG it's acceptable to fail if nothing to ingest.
        raise FileNotFoundError("No CSV files in raw-data folder.")

    chosen_file = random.choice(files)
    file_path = os.path.join(RAW_DATA_PATH, chosen_file)
    print(f"✅ [read_data] Selected file: {file_path}")

    context["ti"].xcom_push(key="file_path", value=file_path)
    return file_path


def validate_data(**context):
    """
    - Does basic Pandas-level checks (rows, missing values)
    - Tries to run Great Expectations validation
    - Combines both into one JSON-serializable dict pushed to XCom
    """
    ti = context["ti"]
    file_path = ti.xcom_pull(key="file_path", task_ids="read_data")
    print(f"🚀 [validate_data] Starting validation for: {file_path}")

    # --- Basic Pandas validation (for row-level splitting) ---
    df = pd.read_csv(file_path)
    nb_rows = len(df)
    invalid_mask = df.isna().any(axis=1)
    nb_invalid_rows = int(invalid_mask.sum())
    nb_valid_rows = int(nb_rows - nb_invalid_rows)

    error_counts = {
        "missing_values": nb_invalid_rows
    }

    invalid_ratio = nb_invalid_rows / nb_rows if nb_rows > 0 else 0.0
    if invalid_ratio > 0.5:
        pandas_criticality = "high"
    elif invalid_ratio > 0.1:
        pandas_criticality = "medium"
    else:
        pandas_criticality = "low"

    print(
        f"📊 [validate_data] rows={nb_rows}, "
        f"invalid={nb_invalid_rows}, invalid_ratio={invalid_ratio:.3f}, "
        f"pandas_criticality={pandas_criticality}"
    )

    # --- Great Expectations validation (wrapped in try/except) ---
    try:
        print("🔁 [validate_data] Running run_ge_validation()...")
        ge_result = run_ge_validation(file_path)
        print("🔍 [GE DEBUG] GE result:", ge_result)
    except Exception as e:
        # Do NOT fail the DAG because of GE issues
        print("❌ [GE ERROR] run_ge_validation failed:", repr(e))
        ge_result = {
            "success": False,
            "total_expectations": 0,
            "successful_expectations": 0,
            "success_ratio": 0.0,
            "criticality": "HIGH",  # treat as serious, you can tweak this
            "report_url": None,
        }

    # --- Combine GE + Pandas criticalities ---
    severity_order = {"low": 1, "medium": 2, "high": 3}
    pandas_sev = severity_order.get(pandas_criticality, 1)
    ge_crit_lower = ge_result.get("criticality", "LOW").lower()
    ge_sev = severity_order.get(ge_crit_lower, 1)

    if pandas_sev >= ge_sev:
        final_criticality = pandas_criticality
    else:
        final_criticality = ge_crit_lower

    print(
        f"✅ [validate_data] Combined criticality -> "
        f"pandas={pandas_criticality}, GE={ge_crit_lower}, final={final_criticality}"
    )

    validation_result = {
        "file_path": file_path,
        "nb_rows": nb_rows,
        "nb_valid_rows": nb_valid_rows,
        "nb_invalid_rows": nb_invalid_rows,
        "error_counts": error_counts,
        # Pandas-based invalid ratio
        "invalid_ratio": invalid_ratio,
        "pandas_criticality": pandas_criticality,
        # Great Expectations info
        "ge_success": bool(ge_result.get("success", False)),
        "ge_success_ratio": ge_result.get("success_ratio", 0.0),
        "ge_criticality": ge_result.get("criticality", "LOW"),
        "ge_report_url": ge_result.get("report_url"),
        # Final severity we will use in alerts / decisions
        "criticality": final_criticality,
    }

    print("📦 [validate_data] validation_result:", validation_result)
    ti.xcom_push(key="validation_result", value=validation_result)
    print("📨 [validate_data] XCom push done")


def save_statistics(**context):
    """
    Save ingestion + GE stats into Postgres.ingestion_stats
    so Grafana can query them.
    """
    ti = context["ti"]
    stats = ti.xcom_pull(key="validation_result", task_ids="validate_data")

    print("📝 [save_statistics] INGESTION STATS:", stats)

    # Connect to Postgres from inside the Docker network
    engine = create_engine(
        "postgresql+psycopg2://postgres:test123@dsp_postgres:5432/dsp_db"
    )

    file_path = stats["file_path"]
    file_name = os.path.basename(file_path)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ingestion_stats (
                    file_path,
                    file_name,
                    nb_rows,
                    nb_valid_rows,
                    nb_invalid_rows,
                    invalid_ratio,
                    pandas_criticality,
                    ge_success,
                    ge_success_ratio,
                    ge_criticality,
                    ge_report_url,
                    criticality
                )
                VALUES (
                    :file_path,
                    :file_name,
                    :nb_rows,
                    :nb_valid_rows,
                    :nb_invalid_rows,
                    :invalid_ratio,
                    :pandas_criticality,
                    :ge_success,
                    :ge_success_ratio,
                    :ge_criticality,
                    :ge_report_url,
                    :criticality
                )
                """
            ),
            {
                "file_path": file_path,
                "file_name": file_name,
                "nb_rows": stats["nb_rows"],
                "nb_valid_rows": stats["nb_valid_rows"],
                "nb_invalid_rows": stats["nb_invalid_rows"],
                "invalid_ratio": stats["invalid_ratio"],
                "pandas_criticality": stats["pandas_criticality"],
                "ge_success": stats["ge_success"],
                "ge_success_ratio": stats["ge_success_ratio"],
                "ge_criticality": stats["ge_criticality"],
                "ge_report_url": stats["ge_report_url"],
                "criticality": stats["criticality"],
            },
        )

    print("✅ [save_statistics] Row inserted into ingestion_stats.")


def send_alerts(**context):
    """
    Generates a simple HTML report and sends an 'alert' to Microsoft Teams
    containing its path and key metrics, with a clickable markdown link.
    """
    ti = context["ti"]
    stats = ti.xcom_pull(key="validation_result", task_ids="validate_data")

    # ---- 1) Build a unique HTML file name ----
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.splitext(os.path.basename(stats["file_path"]))[0]
    report_filename = f"ge_report_{base_name}_{timestamp}.html"
    report_full_path = os.path.join(REPORTS_PATH, report_filename)

    # ---- 2) Build a very simple HTML report ----
    html_content = f"""
    <html>
      <head>
        <title>Data Validation Report - {base_name}</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 20px; }}
          h1 {{ color: #333; }}
          .criticality-high {{ color: #b00020; font-weight: bold; }}
          .criticality-medium {{ color: #ff9800; font-weight: bold; }}
          .criticality-low {{ color: #2e7d32; font-weight: bold; }}
          table {{ border-collapse: collapse; margin-top: 10px; }}
          th, td {{ border: 1px solid #ddd; padding: 8px; }}
          th {{ background-color: #f5f5f5; }}
          code {{ background: #f0f0f0; padding: 2px 4px; border-radius: 4px; }}
        </style>
      </head>
      <body>
        <h1>Data Validation Report</h1>
        <h2>File: {stats["file_path"]}</h2>
        <p>Generated at: {datetime.utcnow().isoformat()} UTC</p>

        <h3>Summary</h3>
        <table>
          <tr><th>Metric</th><th>Value</th></tr>
          <tr><td>Total rows</td><td>{stats["nb_rows"]}</td></tr>
          <tr><td>Invalid rows</td><td>{stats["nb_invalid_rows"]}</td></tr>
          <tr><td>Invalid ratio</td><td>{stats["invalid_ratio"]:.3f}</td></tr>
          <tr><td>Pandas criticality</td><td>{stats["pandas_criticality"]}</td></tr>
          <tr><td>GE success</td><td>{stats["ge_success"]}</td></tr>
          <tr><td>GE success ratio</td><td>{stats["ge_success_ratio"]:.3f}</td></tr>
          <tr><td>GE criticality</td><td>{stats["ge_criticality"]}</td></tr>
          <tr>
            <td>Final criticality</td>
            <td class="criticality-{stats['criticality'].lower()}">
              {stats["criticality"]}
            </td>
          </tr>
        </table>

        <h3>Error counts (Pandas)</h3>
        <pre>{json.dumps(stats["error_counts"], indent=2)}</pre>

        <h3>Notes</h3>
        <ul>
          <li>
            This report combines:
            <ul>
              <li>Row-level missing value checks (Pandas)</li>
              <li>Great Expectations suite result (ge_success_ratio, ge_criticality)</li>
            </ul>
          </li>
          <li>
            If <code>ge_success_ratio &lt; 1</code>, some expectations failed.
          </li>
        </ul>
      </body>
    </html>
    """

    # ---- 3) Write HTML file ----
    with open(report_full_path, "w") as f:
        f.write(html_content)

    ti.xcom_push(key="ge_report_path", value=report_full_path)

    print(f"📄 [send_alerts] HTML report generated at: {report_full_path}")

    # ---- 4) Build a clickable link for Teams ----
    # This will be rendered as markdown in Teams
    report_link_markdown = f"[Open HTML report]({report_full_path})"

    # ---- 5) Build alert message for Teams ----
    text_msg = (
        f"**DATA QUALITY ALERT**\n\n"
        f"- File: `{stats['file_path']}`\n"
        f"- Criticality: **{stats['criticality'].upper()}**\n"
        f"- GE success ratio: `{stats['ge_success_ratio']:.2f}`\n"
        f"- Invalid ratio: `{stats['invalid_ratio']:.3f}`\n"
        f"- Report: {report_link_markdown}\n"
    )

    payload = {"text": text_msg}

    try:
        resp = requests.post(TEAMS_WEBHOOK_URL, json=payload)
        print("📣 [send_alerts] Teams response:", resp.status_code, resp.text)
    except Exception as e:
        print("❌ [send_alerts] Failed to send Teams alert:", repr(e))

    print("📣 [send_alerts] Message sent (or attempted) to Teams:", text_msg)


def split_and_save_data(**context):
    """
    Uses row-level missing-value logic (Pandas) to split good/bad data.
    This is independent from GE but uses the same file.
    """
    ti = context["ti"]
    stats = ti.xcom_pull(key="validation_result", task_ids="validate_data")

    file_path = stats["file_path"]
    df = pd.read_csv(file_path)
    invalid_mask = df.isna().any(axis=1)

    nb_invalid = stats["nb_invalid_rows"]
    nb_rows = stats["nb_rows"]

    print(
        f"🗃️ [split_and_save_data] nb_rows={nb_rows}, "
        f"nb_invalid={nb_invalid} for file={file_path}"
    )

    if nb_invalid == 0:
        # All rows good
        dest = os.path.join(GOOD_DATA_PATH, os.path.basename(file_path))
        df.to_csv(dest, index=False)
        print(f"✅ [split_and_save_data] All rows valid → saved to {dest}")

    elif nb_invalid == nb_rows:
        # All rows bad
        dest = os.path.join(BAD_DATA_PATH, os.path.basename(file_path))
        df.to_csv(dest, index=False)
        print(f"❌ [split_and_save_data] All rows invalid → saved to {dest}")

    else:
        # Mixed → split into two files
        good_df = df[~invalid_mask]
        bad_df = df[invalid_mask]

        good_dest = os.path.join(GOOD_DATA_PATH, "good_" + os.path.basename(file_path))
        bad_dest = os.path.join(BAD_DATA_PATH, "bad_" + os.path.basename(file_path))

        good_df.to_csv(good_dest, index=False)
        bad_df.to_csv(bad_dest, index=False)

        print(
            f"⚖️ [split_and_save_data] Mixed data → "
            f"{len(good_df)} rows → {good_dest}, "
            f"{len(bad_df)} rows → {bad_dest}"
        )


with DAG(
    dag_id="ingest_data_dag",
    start_date=datetime(2025, 11, 1),
    schedule_interval="*/1 * * * *",  # every minute
    catchup=False,
) as dag:

    read_data_task = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
    )

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    save_statistics_task = PythonOperator(
        task_id="save_statistics",
        python_callable=save_statistics,
    )

    send_alerts_task = PythonOperator(
        task_id="send_alerts",
        python_callable=send_alerts,
    )

    split_and_save_data_task = PythonOperator(
        task_id="split_and_save_data",
        python_callable=split_and_save_data,
    )

    # Dependencies
    read_data_task >> validate_data_task
    validate_data_task >> [
        save_statistics_task,
        send_alerts_task,
        split_and_save_data_task,
    ]