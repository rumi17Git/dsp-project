# app/db.py
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.types import JSON
import datetime
import os
import json

DATABASE_URL = "postgresql://postgres:test123@localhost:5432/dsp_project"

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

Base = declarative_base()

class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, index=True)
    customer_identifier = Column(String, nullable=True)
    features_json = Column(Text, nullable=False) 
    predicted_label = Column(Integer, nullable=False)
    predicted_proba = Column(Float, nullable=True)
    source = Column(String, nullable=False) 
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

def init_db():
    Base.metadata.create_all(bind=engine)

def save_prediction(record: dict):

    db = SessionLocal()
    try:
        p = Prediction(
            customer_identifier=record.get("customer_identifier"),
            features_json=json.dumps(record.get("features", {})),
            predicted_label=int(record["predicted_label"]),
            predicted_proba=float(record.get("predicted_proba", 0.0)),
            source=record.get("source", "webapp")
        )
        db.add(p)
        db.commit()
        db.refresh(p)
        return p.id
    finally:
        db.close()

def query_predictions(start_date=None, end_date=None, source=None, limit=1000):
    db = SessionLocal()
    try:
        q = db.query(Prediction)
        if start_date:
            q = q.filter(Prediction.timestamp >= start_date)
        if end_date:
            q = q.filter(Prediction.timestamp <= end_date)
        if source and source != "all":
            q = q.filter(Prediction.source == source)
        q = q.order_by(Prediction.timestamp.desc()).limit(limit)
        results = []
        for r in q.all():
            results.append({
                "id": r.id,
                "customer_identifier": r.customer_identifier,
                "features": json.loads(r.features_json),
                "predicted_label": r.predicted_label,
                "predicted_proba": r.predicted_proba,
                "source": r.source,
                "timestamp": r.timestamp.isoformat()
            })
        return results
    finally:
        db.close()
