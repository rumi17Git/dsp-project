import streamlit as st
import pandas as pd
import requests

API_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="Telco Churn Prediction", layout="wide")
st.title("Telco Customer Churn Prediction")

page = st.sidebar.selectbox("Page", ["Prediction", "Past Predictions"])

if page == "Prediction":

    st.header("Single Prediction")

    with st.form("single_form"):
        col1, col2, col3 = st.columns(3)

        with col1:
            gender = st.selectbox("gender", ["Female", "Male"])
            SeniorCitizen = st.selectbox("SeniorCitizen", [0, 1])
            Partner = st.selectbox("Partner", ["Yes", "No"])
            Dependents = st.selectbox("Dependents", ["Yes", "No"])
            tenure = st.number_input("tenure", min_value=0, value=1)
            PhoneService = st.selectbox("PhoneService", ["Yes", "No"])
            MultipleLines = st.selectbox("MultipleLines", ["No", "Yes", "No phone service"])

        with col2:
            InternetService = st.selectbox("InternetService", ["DSL", "Fiber optic", "No"])
            OnlineSecurity = st.selectbox("OnlineSecurity", ["Yes", "No", "No internet service"])
            OnlineBackup = st.selectbox("OnlineBackup", ["Yes", "No", "No internet service"])
            DeviceProtection = st.selectbox("DeviceProtection", ["Yes", "No", "No internet service"])
            TechSupport = st.selectbox("TechSupport", ["Yes", "No", "No internet service"])
            StreamingTV = st.selectbox("StreamingTV", ["Yes", "No", "No internet service"])
            StreamingMovies = st.selectbox("StreamingMovies", ["Yes", "No", "No internet service"])

        with col3:
            Contract = st.selectbox("Contract", ["Month-to-month", "One year", "Two year"])
            PaperlessBilling = st.selectbox("PaperlessBilling", ["Yes", "No"])
            PaymentMethod = st.selectbox("PaymentMethod", ["Electronic check", "Mailed check",
                                                           "Bank transfer (automatic)", "Credit card (automatic)"])
            MonthlyCharges = st.number_input("MonthlyCharges", min_value=0.0, value=29.85)
            TotalCharges = st.number_input("TotalCharges", min_value=0.0, value=29.85)
            customer_identifier = st.text_input("Customer ID (optional)", value="")

        submit = st.form_submit_button("Predict")

    if submit:
        input_data = pd.DataFrame([{
            "gender": gender,
            "SeniorCitizen": SeniorCitizen,
            "Partner": Partner,
            "Dependents": Dependents,
            "tenure": tenure,
            "PhoneService": PhoneService,
            "MultipleLines": MultipleLines,
            "InternetService": InternetService,
            "OnlineSecurity": OnlineSecurity,
            "OnlineBackup": OnlineBackup,
            "DeviceProtection": DeviceProtection,
            "TechSupport": TechSupport,
            "StreamingTV": StreamingTV,
            "StreamingMovies": StreamingMovies,
            "Contract": Contract,
            "PaperlessBilling": PaperlessBilling,
            "PaymentMethod": PaymentMethod,
            "MonthlyCharges": MonthlyCharges,
            "TotalCharges": TotalCharges,
            "customer_identifier": customer_identifier
        }])

        try:
            response = requests.post(
                f"{API_URL}/predict",
                json={"source": "webapp", "data": input_data.to_dict(orient="records")}
            )
            response.raise_for_status()
            result = response.json()
            st.success(f"Prediction: {result['predictions'][0]} "
                       f"(probability: {result['probabilities'][0]:.2f})")
        except Exception as e:
            st.error(f"Prediction failed: {e}")

    st.header("Batch Prediction (CSV Upload)")

    uploaded_file = st.file_uploader("Upload CSV with all features", type="csv")
    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.dataframe(df.head())

        if st.button("Predict Batch"):
            try:
                # Ensure required columns exist and correct order
                required_cols = [
                    "gender", "SeniorCitizen", "Partner", "Dependents", "tenure",
                    "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity",
                    "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV",
                    "StreamingMovies",
                    "Contract", "PaperlessBilling", "PaymentMethod",
                    "MonthlyCharges", "TotalCharges", "customer_identifier"
                ]

                for col in required_cols:
                    if col not in df.columns:
                        # Fill missing columns with defaults
                        if col in ["SeniorCitizen", "tenure"]:
                            df[col] = 0
                        elif col in ["MonthlyCharges", "TotalCharges"]:
                            df[col] = 0.0
                        else:
                            df[col] = ""

                # Reorder columns
                df = df[required_cols]

                # Convert numeric columns to proper type
                df["SeniorCitizen"] = df["SeniorCitizen"].astype(int)
                df["tenure"] = df["tenure"].astype(int)
                df["MonthlyCharges"] = df["MonthlyCharges"].astype(float)
                df["TotalCharges"] = df["TotalCharges"].astype(float)

                # Send to API
                payload = df.to_dict(orient="records")
                response = requests.post(f"{API_URL}/predict", json=payload)
                response.raise_for_status()
                result = response.json()

                df["predicted_label"] = result["predictions"]
                df["predicted_proba"] = result["probabilities"]
                st.success("Batch prediction completed!")
                st.dataframe(df)

            except Exception as e:
                st.error(f"Batch prediction failed: {e}")



elif page == "Past Predictions":
    st.header("Past Predictions")
    col1, col2, col3 = st.columns(3)

    with col1:
        start_date = st.date_input("Start Date")
    with col2:
        end_date = st.date_input("End Date")
    with col3:
        source = st.selectbox("Source", ["all", "webapp", "scheduled"])

    if st.button("Fetch Past Predictions"):
        try:
            params = {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "source": source
            }
            response = requests.get(f"{API_URL}/past-predictions", params=params)
            response.raise_for_status()
            data = response.json()
            preds = pd.json_normalize(data["predictions"])  
            st.dataframe(preds)
        except Exception as e:
            st.error(f"Failed to fetch past predictions: {e}")
