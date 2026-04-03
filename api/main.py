from pathlib import Path

import pandas as pd
from clickhouse_connect import get_client
from fastapi import FastAPI, HTTPException
from xgboost import XGBClassifier

MODEL_PATH = Path("models/clickstream_xgb.json")
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "analytics"

FEATURE_COLUMNS = [
    "clicks_count",
    "product_views",
    "cart_adds",
    "session_duration",
    "is_mobile",
]

app = FastAPI(title="clickstream analytics api")
model = None


def load_model() -> XGBClassifier:
    clf = XGBClassifier()
    clf.load_model(MODEL_PATH)
    return clf


def fetch_user_features(user_id: int) -> pd.DataFrame:
    query = f"""
    select
        clicks_count,
        product_views,
        cart_adds,
        session_duration,
        is_mobile
    from {CLICKHOUSE_DATABASE}.user_features
    where user_id = %(user_id)s
    order by event_date desc
    limit 1
    """

    client = get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    try:
        rows = client.query_df(query, parameters={"user_id": user_id})
    finally:
        client.close()

    return rows


@app.on_event("startup")
def startup() -> None:
    global model

    if MODEL_PATH.exists():
        model = load_model()


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "model_loaded": model is not None,
        "model_path": str(MODEL_PATH),
    }


@app.get("/predict/{user_id}")
def predict(user_id: int) -> dict:
    if model is None:
        raise HTTPException(status_code=503, detail="model file not found, run airflow dag first")

    features = fetch_user_features(user_id)
    if features.empty:
        raise HTTPException(status_code=404, detail=f"no features found for user_id={user_id}")

    proba = float(model.predict_proba(features[FEATURE_COLUMNS])[0][1])

    return {
        "user_id": user_id,
        "will_buy": proba >= 0.5,
        "purchase_probability": round(proba, 4),
        "features": features.iloc[0].to_dict(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
