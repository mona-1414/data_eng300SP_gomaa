# import required packages
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import random

# 1. Define the workflow schedule frequency
WORKFLOW_SCHEDULE = "*/3 * * * *"  # every 3 minutes

# 2. Default arguments dictionary for the DAG execution
default_args = {
    "owner": "dinglin",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 1,
}

# 3. Task Definitions
def task1_func(**context):
    print("Running task 1")
    value = "success" if random.random() < 0.5 else "failure"
    print(f"Task 1 output: {value}")
    return {"status": value}

def task2_func(**context):
    print("Running task 2")
    value = random.randint(0, 10)
    print(f"Task 2 output: {value}")
    return {"value": value}

def task3_func(**context):
    print("Running task 3")
    value_one = random.randint(0, 10)
    value_two = random.randint(0, 10)
    print(f"Task 3 output: {value_one} {value_two}")
    return {"value1": value_one, "value2": value_two}

def task4_func(**context):
    print("Running task 4")
    ti = context["ti"]

    task1_return_value = ti.xcom_pull(task_ids="task1")
    task2_return_value = ti.xcom_pull(task_ids="task2")
    task3_return_value = ti.xcom_pull(task_ids="task3")

    print("Task 1 returned:", task1_return_value)
    print("Task 2 returned:", task2_return_value)
    print("Task 3 returned:", task3_return_value)

    if (
        task3_return_value["value1"] > task3_return_value["value2"]
        and task1_return_value["status"] == "success"
    ):
        return "do-task5"

    return ""

def decide_which_path(**context):
    ti = context["ti"]
    task4_return_value = ti.xcom_pull(task_ids="task4")
    return "task5" if task4_return_value == "do-task5" else "dummy_task"

def task5_func(**context):
    print("Running task 5")
    return {"task": "task5", "status": "completed"}


# 4/5/6. DAG + tasks + dependencies (with-context style)
with DAG(
    dag_id="example_dag2",
    default_args=default_args,
    description="An example DAG with dependencies",
    schedule=WORKFLOW_SCHEDULE,
    tags=["de300"],
    catchup=False,  # optional; keep/remove depending on your teaching goal
) as dag:

    task1 = PythonOperator(
        task_id="task1",
        python_callable=task1_func,
    )

    task2 = PythonOperator(
        task_id="task2",
        python_callable=task2_func,
    )

    task3 = PythonOperator(
        task_id="task3",
        python_callable=task3_func,
    )

    task4 = PythonOperator(
        task_id="task4",
        python_callable=task4_func,
    )

    decide = BranchPythonOperator(
        task_id="branch_task",
        python_callable=decide_which_path,
    )

    task5 = PythonOperator(
        task_id="task5",
        python_callable=task5_func,
    )

    dummy_task = EmptyOperator(
        task_id="dummy_task",
    )

    # dependencies
    [task1, task2, task3] >> task4 >> decide
    decide >> [task5, dummy_task]