from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import pandas as pd
import numpy as np
import json

WORKFLOW_SCHEDULE = "@daily"

default_args = {
    "owner": "dinglin",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 1,
}

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
def make_data(**context):
    """
    Create a toy dataset with missing values + a noisy numeric column.
    Output: JSON string representing records.
    """
    rng = np.random.default_rng(42)
    n = 200

    df = pd.DataFrame({
        "user_id": rng.integers(1, 51, size=n),
        "amount": rng.normal(loc=50, scale=20, size=n).round(2),
        "category": rng.choice(["A", "B", "C"], size=n, p=[0.5, 0.3, 0.2]),
        "event_ts": pd.Timestamp.utcnow().floor("D") - pd.to_timedelta(rng.integers(0, 14, size=n), unit="D"),
    })

    # Inject some missing values
    missing_idx = rng.choice(df.index, size=15, replace=False)
    df.loc[missing_idx, "amount"] = np.nan

    print("Raw head:\n", df.head())
    return df_to_xcom(df)

def clean_and_feature(**context):
    """
    Clean missing values and add features.
    - Fill missing amount with median
    - Add is_big_spend flag
    """
    ti = context["ti"]
    raw_payload = ti.xcom_pull(task_ids="make_data")
    df = xcom_to_df(raw_payload)

    df["event_ts"] = pd.to_datetime(df["event_ts"])
    median_amt = df["amount"].median()
    df["amount"] = df["amount"].fillna(median_amt)

    df["is_big_spend"] = (df["amount"] >= 70).astype(int)
    df["day"] = df["event_ts"].dt.date

    print("Cleaned head:\n", df.head())
    return df_to_xcom(df)

def summarize(**context):
    """
    Aggregate summary stats.
    Output: dict summary for branching + logging.
    """
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="clean_and_feature")
    df = xcom_to_df(payload)

    summary = {
        "rows": int(len(df)),
        "unique_users": int(df["user_id"].nunique()),
        "mean_amount": float(df["amount"].mean()),
        "big_spend_rate": float(df["is_big_spend"].mean()),
        "total_amount": float(df["amount"].sum()),
    }
    print("Summary:", summary)
    return summary

def decide_path(**context):
    """
    Branch if big_spend_rate is high.
    - if >= 0.30 -> "save_alert"
    - else -> "save_normal"
    """
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="summarize") or {}
    rate = summary.get("big_spend_rate", 0.0)

    if rate >= 0.30:
        return "save_alert"
    return "save_normal"

def save_normal(**context):
    """
    Pretend to save standard report.
    """
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="summarize")
    print("Saving NORMAL report:", summary)
    return {"saved": "normal", "summary": summary}

def save_alert(**context):
    """
    Pretend to save alert report.
    """
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="summarize")
    print("Saving ALERT report:", summary)
    return {"saved": "alert", "summary": summary}

# ---------- DAG definition ----------
with DAG(
    dag_id="pandas_example_dag",
    default_args=default_args,
    description="Example DAG that uses pandas for ETL + branching",
    schedule=WORKFLOW_SCHEDULE,
    catchup=False,
    tags=["de300", "pandas"],
) as dag:

    t_make = PythonOperator(
        task_id="make_data",
        python_callable=make_data,
    )

    t_clean = PythonOperator(
        task_id="clean_and_feature",
        python_callable=clean_and_feature,
    )

    t_sum = PythonOperator(
        task_id="summarize",
        python_callable=summarize,
    )

    t_branch = BranchPythonOperator(
        task_id="branch_on_metric",
        python_callable=decide_path,
    )

    t_save_normal = PythonOperator(
        task_id="save_normal",
        python_callable=save_normal,
    )

    t_save_alert = PythonOperator(
        task_id="save_alert",
        python_callable=save_alert,
    )

    t_done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    t_make >> t_clean >> t_sum >> t_branch
    t_branch >> [t_save_normal, t_save_alert] >> t_done