from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

from datetime import datetime

# DAG 정의
default_args = {
    # 'owner': 'airflow',
    # 'start_date': days_ago(1),
    'start_date': datetime(2024,6,12),
    # 'retries': 1,
}

dag = DAG(
    'kafka_batch',
    default_args=default_args,
    description='kafka data pull&push BashOperator',
    tags=["yahoo_finance", "kafka", "conumer","producer","BashOperator"],
    #schedule_interval='0 10 * * *',
    schedule_interval='NONE',
)

# 각 Python 스크립트를 실행하는 BashOperator
kafka_producer = BashOperator(
    task_id='kafka_producer',
    bash_command='python /opt/airflow/scripts/producer_batch.py',
    dag=dag,
)

kafka_consumer = BashOperator(
    task_id='kafka_consumer',
    bash_command='python /opt/airflow/scripts/consumer_batch.py',
    dag=dag,
)

# kafka_producer = KubernetesPodOperator(
#     task_id='kafka_producer',
#     name='kafka_producer',
#     namespace='default',
#     image='python:3.8-slim',
#     cmds=["python", "producer_batch.py"],
#     arguments=["print('start producer_batch.py')"],  # or use a script file
#     get_logs=True,
#     dag=dag,
# )



# 태스크 의존성 설정 (예: 모든 스크립트를 병렬로 실행)
kafka_producer >> kafka_consumer

# DAG 정의 완료
if __name__ == "__main__":
    dag.cli()



# # 각 Python 스크립트를 실행하는 KubernetesPodOperator
# run_script1 = KubernetesPodOperator(
#     task_id='run_script1',
#     name='run-script1',
#     namespace='default',
#     image='python:3.8-slim',
#     cmds=["python", "-c"],
#     arguments=["print('Hello from script1')"],  # or use a script file
#     get_logs=True,
#     dag=dag,
# )

