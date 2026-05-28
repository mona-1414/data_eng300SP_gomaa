from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import pandas as pd
import numpy as np
import json
import io
import boto3
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

SOURCE_BUCKET = "dinglin-spring26"
SOURCE_KEY = "lab6/cars.csv"

OUTPUT_BUCKET = "lab6-monagomaa"
OUTPUT_PREFIX = "lab6/output"

WORKFLOW_SCHEDULE = "@daily"

default_args = {
    "owner": "monagomaa",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 1,
}

FEATURES = ["Weight", "Drive_Ratio", "Horsepower", "Displacement", "Cylinders"]
TARGET = "MPG"

# ---------- Helpers ----------
def df_to_xcom(df: pd.DataFrame) -> str:
    """Serialize DataFrame to JSON string for XCom."""
    return df.to_json(orient="records")

def xcom_to_df(payload: str) -> pd.DataFrame:
    """Deserialize JSON string from XCom back to DataFrame."""
    if not payload:
        return pd.DataFrame()
    return pd.DataFrame(json.loads(payload))

# ---------- Task callables ----------

def read_data(**context):

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    
    print(f"Loaded {len(df)} rows, columns: {list(df.columns)}")
    print(df.head())
    
    return df.to_json(orient="records")

def split_data(**context):
    
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="read_data")
    df = pd.DataFrame(json.loads(raw))
    
    df = df[FEATURES + [TARGET]].dropna()
    
    X = df[FEATURES]
    
    y = df[TARGET]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")
 
    payload = {
        "X_train": X_train.to_json(orient="records"),
        "X_test": X_test.to_json(orient="records"),
        "y_train": y_train.tolist(),
        "y_test": y_test.tolist(),
    }
    return json.dumps(payload)
    
def train_model(**context):
 
    ti = context["ti"]
    payload = json.loads(ti.xcom_pull(task_ids="split_data"))
 
    X_train = pd.DataFrame(json.loads(payload["X_train"]))
    y_train = payload["y_train"]
 
    model = LinearRegression()
    model.fit(X_train, y_train)
 
    print("Coefficients:", dict(zip(FEATURES, model.coef_)))
    print("Intercept:", model.intercept_)
 
    model_params = {
        "coef": model.coef_.tolist(),
        "intercept": model.intercept_,
        "features": FEATURES,
    }
    return json.dumps(model_params)   

def evaluate_model(**context):
    import pandas as pd
    import json
    import boto3
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error, r2_score
    import numpy as np
 
    ti = context["ti"]
    ds = context["ds"]  
 
    payload = json.loads(ti.xcom_pull(task_ids="split_data"))
    model_params = json.loads(ti.xcom_pull(task_ids="train_model"))
 
    X_test = pd.DataFrame(json.loads(payload["X_test"]))
    y_test = payload["y_test"]
 
    model = LinearRegression()
    model.coef_ = np.array(model_params["coef"])
    model.intercept_ = model_params["intercept"]
 
    y_pred = model.predict(X_test)
 
    mse = mean_squared_error(y_test, y_pred)
    rmse = float(np.sqrt(mse))
    r2 = r2_score(y_test, y_pred)
 
    metrics = {
        "ds": ds,
        "test_size": len(y_test),
        "mse": float(mse),
        "rmse": rmse,
        "r2": float(r2),
        "features": FEATURES,
        "target": TARGET,
        "coefficients": dict(zip(FEATURES, model_params["coef"])),
        "intercept": model_params["intercept"],
    }
 
    print("Metrics:", metrics)
 
    output_key = f"{OUTPUT_PREFIX}/dt={ds}/metrics.json"
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=output_key,
        Body=json.dumps(metrics, indent=2),
        ContentType="application/json",
    )
    print(f"Metrics written to s3.")
    return metrics

# ---------- DAG definition ----------
with DAG(
    dag_id="lab6",
    default_args=default_args,
    description="lab 6 training linear regression model",
    schedule=WORKFLOW_SCHEDULE,
    catchup=False,
    tags=["de300", "lab6"],
) as dag:

    t_read = PythonOperator(
            task_id="read_data",
            python_callable=read_data,
    )
    
    t_split = PythonOperator(
            task_id="split_data",
            python_callable=split_data,
    )
    
    t_train = PythonOperator(
            task_id="train_model",
            python_callable=train_model,
    )
    
    t_evaluate = PythonOperator(
            task_id="evaluate_model",
            python_callable=evaluate_model,
    )
    
    t_read >> t_split >> t_train >> t_evaluate  