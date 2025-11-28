# airflow/dags/ge_validation.py

import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import great_expectations as gx


def run_ge_validation(csv_path: str) -> dict:
    """
    Real Great Expectations helper.

    - Reads the CSV into a pandas DataFrame
    - Builds a GE Validator from the DataFrame
    - Applies a few expectations (nulls, ranges, categories…)
    - Computes a success ratio and criticality (LOW/MEDIUM/HIGH)
    - Writes a small HTML report and returns its path
    """

    # ---- Load data ---------------------------------------------------------
    df = pd.read_csv(csv_path)
    n_rows, n_cols = df.shape

    # Great Expectations "in-memory" validator from pandas DataFrame
    validator = gx.from_pandas(df)

    # ---- Define expectations -----------------------------------------------
    # Adjust these to match your dataset columns
    required_not_null = [
        "customerID",
        "gender",
        "tenure",
        "MonthlyCharges",
    ]

    expectations_results = []

    # 1) Not-null expectations for some key columns
    for col in required_not_null:
        if col in df.columns:
            res = validator.expect_column_values_to_not_be_null(col)
            expectations_results.append(res)

    # 2) Numeric ranges (if columns exist)
    if "tenure" in df.columns:
        res = validator.expect_column_values_to_be_between(
            "tenure",
            min_value=0,
            max_value=1000,
            allow_cross_type_comparisons=True,
        )
        expectations_results.append(res)

    if "MonthlyCharges" in df.columns:
        res = validator.expect_column_values_to_be_between(
            "MonthlyCharges",
            min_value=0,
            max_value=None,
            allow_cross_type_comparisons=True,
        )
        expectations_results.append(res)

    # 3) Optional: categorical expectation (only if column exists)
    if "gender" in df.columns:
        res = validator.expect_column_values_to_be_in_set(
            "gender",
            value_set=["Male", "Female"],
            result_format="BASIC",
        )
        expectations_results.append(res)

    # ---- Compute success ratio ---------------------------------------------
    total_expectations = len(expectations_results)

    if total_expectations == 0:
        success_ratio = 1.0
        successful_expectations = 0
    else:
        def _is_success(r):
            # GE Result objects can be either objects or dicts, be defensive
            if hasattr(r, "success"):
                return bool(r.success)
            if isinstance(r, dict):
                return bool(r.get("success", False))
            return False

        successes = [r for r in expectations_results if _is_success(r)]
        successful_expectations = len(successes)
        success_ratio = successful_expectations / total_expectations

    # ---- Map success_ratio -> criticality ----------------------------------
    # You can tune these thresholds
    if success_ratio < 0.6:
        ge_criticality = "HIGH"
    elif success_ratio < 0.9:
        ge_criticality = "MEDIUM"
    else:
        ge_criticality = "LOW"

    # ---- Build a very simple HTML report -----------------------------------
    reports_root = "/opt/airflow/logs/ge_reports"
    os.makedirs(reports_root, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = Path(csv_path).stem
    report_path = Path(reports_root) / f"ge_report_{stem}_{ts}.html"

    html_rows = []
    for r in expectations_results:
        if hasattr(r, "expectation_config"):
            name = r.expectation_config.expectation_type
            kwargs = r.expectation_config.kwargs
            success_flag = getattr(r, "success", False)
        elif isinstance(r, dict):
            cfg = r.get("expectation_config", {})
            name = cfg.get("expectation_type", "unknown_expectation")
            kwargs = cfg.get("kwargs", {})
            success_flag = r.get("success", False)
        else:
            name = str(r)
            kwargs = {}
            success_flag = False

        html_rows.append(
            f"<tr><td>{name}</td>"
            f"<td>{success_flag}</td>"
            f"<td><pre>{kwargs}</pre></td></tr>"
        )

    html_content = f"""
    <html>
      <head>
        <title>GE Report for {stem}</title>
        <meta charset="utf-8" />
      </head>
      <body>
        <h1>Great Expectations Report</h1>
        <p><b>File:</b> {csv_path}</p>
        <p><b>Rows:</b> {n_rows} | <b>Columns:</b> {n_cols}</p>
        <p><b>Total expectations:</b> {total_expectations}</p>
        <p><b>Successful expectations:</b> {successful_expectations}</p>
        <p><b>Success ratio:</b> {success_ratio:.2f}</p>
        <p><b>GE criticality:</b> {ge_criticality}</p>
        <table border="1" cellspacing="0" cellpadding="4">
            <tr>
              <th>Expectation</th>
              <th>Success</th>
              <th>Kwargs</th>
            </tr>
            {''.join(html_rows)}
        </table>
      </body>
    </html>
    """

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # ---- Return structured result for the DAG ------------------------------
    return {
        "success": success_ratio == 1.0,
        "total_expectations": total_expectations,
        "successful_expectations": successful_expectations,
        "success_ratio": float(success_ratio),
        "criticality": ge_criticality,       # "LOW" / "MEDIUM" / "HIGH"
        "report_url": str(report_path),      # full path to HTML in Airflow logs
        "validated_file": csv_path,
    }