# Data Science Project: Automated ML Pipeline

## Project Overview

This project implements an **automated machine learning pipeline** to process new data, train a model, evaluate performance, and serve predictions via a **FastAPI endpoint**. The pipeline is fully automated using **Apache Airflow**, enabling seamless integration of new data and batch predictions.

This project focuses on customer churn prediction for a telecommunications company. The goal is to develop a machine learning model that predicts whether a customer will discontinue their subscription using demographic, service, and billing data. Accurately identifying potential churners allows the company to implement proactive retention strategies, such as targeted offers or improved customer support, ultimately reducing revenue loss and enhancing customer loyalty.

**Key Features:**
- Data ingestion and preprocessing
- Model training, validation, and evaluation
- F1-score, Accuracy, Precision, and Recall computation
- Confusion matrix for test set predictions
- FastAPI endpoint for real-time predictions
- Airflow DAG for automation

---

## Project Structure

---

## Installation

1. **Clone the repository:**
```bash
git clone <your-repo-url>
cd project-root

	2.	Create a virtual environment (conda or venv):
conda create -n ml_env python=3.11
conda activate ml_env

	3.	Install dependencies:

pip install -r requirements.txt

Data Pipeline
	1.	Data Ingestion:
The DAG monitors the good-data/ folder for new CSV files.
	2.	Preprocessing:
	•	Handle missing values
	•	Encode categorical variables
	•	Prepare feature matrix and target vector
	3.	Model Training:
	•	Train a classification model (e.g., Random Forest, XGBoost)
	•	Validate using train/test split
	•	Compute metrics: Accuracy, Precision, Recall, F1-score
	4.	Prediction:
	•	Once the model achieves F1 > 0.75 on validation, predict on test set
	•	Generate Confusion Matrix

FastAPI Deployment
	•	The FastAPI server exposes an endpoint for predictions:
POST /predict

Start FastAPI server:
uvicorn app.main:app --reload

Airflow DAG Automation
	•	DAG automatically:
	1.	Checks good-data/ for new files
	2.	Sends new data to FastAPI for prediction
	3.	Logs results to database or CSV

Start Airflow scheduler & webserver:
airflow db init
airflow webserver --port 8085
airflow scheduler

Access Airflow UI: http://localhost:8085

Evaluation Metrics
	•	Accuracy: Measures overall correctness
	•	Precision: Measures the correctness of positive predictions
	•	Recall: Measures coverage of actual positives
	•	F1-score: Weighted balance between Precision and Recall
	•	Confusion Matrix: Detailed breakdown of predictions

Target: F1-score ≥ 0.75 on validation data



