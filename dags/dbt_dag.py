from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup 
from datetime import datetime, timedelta

default_args = {
    'owner': 'Mahmoud',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_snowflake_pipeline',
    default_args=default_args,
    description='Run dbt models using dbt core',
    schedule_interval='@daily',
    catchup=False,
    tags=['DBT', 'Snowflake', 'Pipeline']
) as dag:

    Start=DummyOperator(task_id="Start")
    with TaskGroup("dbt_tasks", tooltip="DBT commands") as dbt_group:
        dbt_run = BashOperator(
            task_id='dbt_run',
            bash_command='cd /opt/airflow/dbt && dbt run',
            doc_md="""
             ### dbt_run
             This task runs the dbt models using `dbt run`.
             It ensures all transformations are applied on Snowflake.
             """,
        )

        dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command='cd /opt/airflow/dbt && dbt test',
            doc_md="""
            ### dbt_test
            This task runs `dbt test` to validate data models using tests.
            """,
        )
    End=DummyOperator(task_id="End")
    Start >>dbt_run >> dbt_test >>End


