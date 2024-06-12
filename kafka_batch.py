from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime

# DAG 정의
default_args = {
    # 'owner': 'airflow',
    'start_date': days_ago(1),
    # 'retries': 1,
}

dag = DAG(
    'kafka_batch',
    default_args=default_args,
    description='kafka data pull&push BashOperator',
    schedule_interval='None',
    # schedule_interval='@daily',
)

# 각 Python 스크립트를 실행하는 BashOperator
kafka_producer = BashOperator(
    task_id='kafka_producer',
    bash_command='python3 /opt/airflow/scripts/producer_batch.py',
    dag=dag,
)

# kafka_consumer = BashOperator(
#     task_id='kafka_consumer',
#     bash_command='python /opt/airflow/scripts/consumer_batch.py',
#     dag=dag,
# )

# 태스크 의존성 설정 (예: 모든 스크립트를 병렬로 실행)
kafka_producer #>> kafka_consumer

# DAG 정의 완료
if __name__ == "__main__":
    dag.cli()
