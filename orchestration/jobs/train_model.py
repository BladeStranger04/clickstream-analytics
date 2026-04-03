from clickhouse_driver import Client
from xgboost import XGBClassifier


def train_model():
    client = Client("clickhouse")
    data = client.query_dataframe("SELECT * FROM user_features")

    data["target"] = (data["url"] == "/checkout").astype(int)

    X = data[["clicks_count", "is_mobile", "session_duration"]]
    y = data["target"]

    model = XGBClassifier()
    model.fit(X, y)
    model.save_model("/models/click_v1.json")