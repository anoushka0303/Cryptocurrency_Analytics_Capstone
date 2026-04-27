from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

def success_callback(context):
    print(f"SUCCESS: {context['task_instance'].task_id}")

def failure_callback(context):
    print(f"FAILED: {context['task_instance'].task_id}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': success_callback,
    'on_failure_callback': failure_callback
}

with DAG(
    dag_id='crypto_pipeline',
    default_args=default_args,
    description='Bronze -> Silver -> Gold pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    bronze = DatabricksRunNowOperator(
        task_id='bronze_task',
        databricks_conn_id='databricks_default',
        job_id=934528138875571
    )

    silver = DatabricksRunNowOperator(
        task_id='silver_task',
        databricks_conn_id='databricks_default',
        job_id=491941341352503
    )

    gold = DatabricksRunNowOperator(
        task_id='gold_task',
        databricks_conn_id='databricks_default',
        job_id=867004233847440
    )

    # Final success task (runs only if ALL succeed)
    success = EmptyOperator(
        task_id='pipeline_success'
    )

    # Final failure task (runs if ANY fails)
    failure = EmptyOperator(
        task_id='pipeline_failure',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    bronze >> silver >> gold

    gold >> success
    [bronze, silver, gold] >> failure