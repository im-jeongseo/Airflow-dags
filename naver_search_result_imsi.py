# 필요한 모듈 Import
from datetime import datetime
from airflow import DAG
import json
# from preprocess.naver_preprocess import preprocessing

# 사용할 Operator Import
# from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator 
from airflow.utils.dates import days_ago

from pandas import json_normalize
import pandas as pd
from sqlalchemy import create_engine
import os

# 디폴트 설정
default_args = {
    "start_date": datetime(2022, 1, 1) # 2022년 1월 1일 부터 대그 시작 --> 현재는 22년 7월이므로 대그를 실행하면 무조건 한 번은 돌아갈 것
}

# 본인이 발급받은 키를 넣으세요
NAVER_CLI_ID = "Y8iY4rHUwO8J48PBQOjv"
NAVER_CLI_SECRET = "ckkvO4Q3AW"

def _complete():
    print("네이버 검색 DAG 완료")

def preprocessing(ti):
    # ti(task instance) dag 내의 task의 정보를 얻어 낼 수 있는 객체

    # xcom(cross communication) - Operator와 Operator 사이에 데이터를 전달 할 수 있게끔 하는 도구
    search_result = ti.xcom_pull(task_ids=["crawl_naver"])

    # xcom을 이용해 가지고 온 결과가 없는 경우
    if not len(search_result):
        raise ValueError("검색 결과 없음")
    
    items = search_result[0]["items"]
    processed_items = json_normalize([
        {"title": item["title"],
         "address": item["address"],
         "category": item["category"],
         "description": item["description"],
         "link": item["link"]} for item in items
    ])
    csv_path = '/opt/airflow/naver_processed_result.csv'
    #processed_items.to_csv (csv_path, index=None, header=False)
    processed_items.to_csv (csv_path, index=None)

    df = pd.read_csv(csv_path)
    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine('postgresql://postgres:postgres@192.168.168.133:30032/stock')
    # Replace 'table_name' with your desired table name
    df.to_sql('naver_search_result', engine, if_exists='replace', index=False)


def load_csv_to_postgres(ti):
    csv_path = ti.xcom_pull(task_ids=["preprocess_result"], key='csv_path')
    
    if os.path.exists(csv_path[0]):
        # Process the file
        print(f"CSV file path received: {csv_path[0]}")
    else:
        raise FileNotFoundError(f"File not found at {csv_path[0]}")

    df = pd.read_csv(csv_path[0])
    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine('postgresql://postgres:postgres@192.168.168.133:30032/stock')
    # Replace 'table_name' with your desired table name
    df.to_sql('naver_search_result', engine, if_exists='append', index=False)

# DAG 틀 설정
with DAG(
    dag_id="naver-search-pipeline-imsi",
    # crontab 표현 사용 가능 https://crontab.guru/
    schedule_interval="@daily", 
    default_args=default_args,
    # 태그는 원하는대로
    tags=["naver", "search", "local", "api", "pipeline"],
    # catchup을 True로 하면, start_date 부터 현재까지 못돌린 날들을 채운다
    catchup=False) as dag:

    # 네이버 API로 지역 식당을 검색할 것이다. 
    # 지역 식당명, 주소, 카테고리, 설명, 링크를 저장할 것이므로 다음과 같이 테이블을 구성한다.
    creating_table = PostgresOperator(
        task_id="creating_table",
        postgres_conn_id='stock_test',
        # naver_search_result 라는 테이블이 없는 경우에만 만들도록 IF NOT EXISTS 조건을 넣어주자.
        sql="""
            CREATE TABLE IF NOT EXISTS naver_search_result( 
                title VARCHAR,
                address VARCHAR,
                category VARCHAR,
                description VARCHAR,
                link VARCHAR
            );
        """,
    )

    # HTTP 센서를 이용해 응답 확인 (감지하는 오퍼레이터로 실제 데이터를 가져오는 것 X)
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="naver_search_api",
        endpoint="v1/search/local.json", # url - uri에서 Host 부분을 제외한 파트(~.com 까지가 host)
        # 요청 헤더, -H 다음에 오는 내용들
        headers={
            "X-Naver-Client-Id" : f"{NAVER_CLI_ID}",
            "X-Naver-Client-Secret" : f"{NAVER_CLI_SECRET}",
        },
        request_params={
            "query": "주식",
            "display": 5
        }, # 요청 변수
        response_check=lambda response: response.json() # 응답 확인
    )
    
    # 네이버 검색 결과를 가져올 오퍼레이터를 만든다.
    crawl_naver = SimpleHttpOperator(
        task_id="crawl_naver",
        http_conn_id="naver_search_api",
        endpoint="v1/search/local.json", # url 설정
        headers={
            "X-Naver-Client-Id" : f"{NAVER_CLI_ID}",
            "X-Naver-Client-Secret" : f"{NAVER_CLI_SECRET}",
        }, # 요청 헤더
        data={
            "query": "주식",
            "display": 5
        }, # 요청 변수
        method="GET", # 통신 방식 GET, POST 등등 맞는 것으로
        response_filter=lambda res : json.loads(res.text),
        log_response=True
    )
    # 검색 결과 전처리하고 CSV 저장
    preprocess_result = PythonOperator(
            task_id="preprocess_result",
            python_callable=preprocessing, # 실행할 파이썬 함수
            provide_context=True,
            dag=dag,
    )
    
    #check_csv = BashOperator(
    #    task_id="check_csv",
    #    bash_command = 'hostname; pwd; ls -al'
    #)

    #store_result = PythonOperator(
    #        task_id='store_result',
    #        python_callable=load_csv_to_postgres,
    #        provide_context=True,
    #        dag=dag,
    #)

    # 대그 완료 출력
    print_complete = PythonOperator(
            task_id="print_complete",
            python_callable=_complete # 실행할 파이썬 함수
    )

    # 파이프라인 구성하기
    creating_table >> is_api_available >> crawl_naver >> preprocess_result >> print_complete