from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
import pandas as pd

with DAG(
    dag_id='ex_dags_bash_operator', # 웹에서 보이는 DAG 이름(python 파일명과는 상관 없음.)
    schedule_interval='0 0 * * *', # 분,시,일,월,요일
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False, # start_date - 현재 시간 구간에 누락된 task를 실행할지 안할지 결정(False는 실행 X)
    # dagrun_timeout=datetime.timedelta(minutes=60), # 60분 이상 되면 멈춤
    tags=['example', 'example2'], # 웹에서 보이는 tag 값 설정 (나중에 검색에 용이함)
    params={"example_key": "example_value"},
) as dag:
    
    bash_t1 = BashOperator(
        task_id='bash_t1', # DAG 안에서 보이는 객체명
        bash_command='echo whoami', # 실행할 쉘 스크립트
    )

    bash_t2 = BashOperator(
        task_id='bash_t2', # DAG 안에서 보이는 객체명
        bash_command='echo $HOSTNAME', # 실행할 쉘 스크립트
    )

    bash_t1 >> bash_t2 # task가 돌아가는 순서 결정 t1 이후에 t2 실행


