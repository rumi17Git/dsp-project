from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
import datetime
import os
import json
import math  # ✅ for isnan and isinf checks

# ✅ Ensure credentials match docker-compose.yml
DATABASE_URL = "postgresql://postgres:test123@dsp_postgres:5432/dsp_db"

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
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)

def _sanitize_float(value):
    """Convert invalid float values (NaN, inf) to 0.0"""
    try:
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return val
    except (ValueError, TypeError):
        return 0.0

def save_prediction(record: dict):
    """Save a single prediction record to the database"""
    db = SessionLocal()
    try:
        p = Prediction(
            customer_identifier=record.get("customer_identifier"),
            features_json=json.dumps(record.get("features", {})),
            predicted_label=int(record.get("predicted_label", 0)),
            predicted_proba=_sanitize_float(record.get("predicted_proba")),
            source=record.get("source", "webapp")
        )
        db.add(p)
        db.commit()
        db.refresh(p)
        return p.id
    finally:
        db.close()

def query_predictions(start_date=None, end_date=None, source=None, limit=1000):
    """Query past predictions with filters"""
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
