import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetimes import datetime, timedelta

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'cuinv',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='imsi_dag_v0.0.1',
    default_args=default_args,
    schedule='1 * * * *',
    start_datae=datetime(2023, 2 ,15, tzinfo=local_tz),
    catchup=False
)

start_task = BashOperator(
    task_id='start_task',
    bash_command="echo 12345",
    dag=dag
)

echo_task = BashOperator(
    task_id='echo_task',
    bash_command="echo 678"
    dag=dag
)

end_task = BashOperator(
    task_id='end_task',
    bash_command="echo 8910",
    dag=dag
)

start_task >> echo_task >> end_task