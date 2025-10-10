from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import json
import datetime
from app.db import SessionLocal, Prediction
import joblib
import traceback

app = FastAPI(title="Telco Churn Model Service")

model = joblib.load("models/model.joblib")

class PredictRequest(BaseModel):
    source: str 
    data: List[dict] 


@app.post("/predict")
def predict(req: PredictRequest):
    if len(req.data) == 0:
        raise HTTPException(status_code=400, detail="No data provided")

    try:
        df = pd.DataFrame(req.data)
        preds = model.predict(df)
        probs = model.predict_proba(df)[:, 1]

        db = SessionLocal()
        for i, row in df.iterrows():
            record = Prediction(
                customer_identifier=row.get("customer_identifier", None),
                features_json=json.dumps(row.to_dict()),
                predicted_label=int(preds[i]),
                predicted_proba=float(probs[i]),
                source=req.source,
                timestamp=datetime.datetime.utcnow()
            )
            db.add(record)
        db.commit()
        db.close()

        return {
            "predictions": preds.tolist(),
            "probabilities": probs.tolist()
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.get("/past-predictions")
def past_predictions(
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    source: Optional[str] = Query("all", description="webapp | scheduled | all")
):
    try:
        db = SessionLocal()
        query = db.query(Prediction)

        if source in ["webapp", "scheduled"]:
            query = query.filter(Prediction.source == source)

        if start_date:
            start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            query = query.filter(Prediction.timestamp >= start_dt)
        if end_date:
            end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d") + datetime.timedelta(days=1)
            query = query.filter(Prediction.timestamp < end_dt)

        results = query.order_by(Prediction.timestamp.desc()).all()
        db.close()

        response = []
        for r in results:
            response.append({
                "id": r.id,
                "customer_identifier": r.customer_identifier,
                "features": json.loads(r.features_json),
                "predicted_label": r.predicted_label,
                "predicted_proba": r.predicted_proba,
                "source": r.source,
                "timestamp": r.timestamp.isoformat()
            })

        return {"predictions": response}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching past predictions: {str(e)}")
