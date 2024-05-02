from datetime import datetime
from airflow import DAG

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

import requests
#import pandas_datareader as pdr
from bs4 import BeautifulSoup as bs


def get_code(str):
    code_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]
    # 종목코드가 6자리이기 때문에 6자리를 맞춰주기 위해 설정해줌
    code_df.종목코드 = code_df.종목코드.map('{:06d}'.format)
    # 우리가 필요한 것은 회사명과 종목코드이기 때문에 필요없는 column들은 제외해준다.
    code_df = code_df[['회사명', '종목코드']]
    # 한글로된 컬럼명을 영어로 바꿔준다.
    code_df = code_df.rename(columns={'회사명': 'name', '종목코드': 'code'})

    test_cd = code_df[code_df['name'] == str]
    code = test_cd['code'].iloc[0]
    return code



# 종목,페이지수로 크롤링
def get_day_list(item_code, page_no):
    url = f"https://finance.naver.com/item/sise_day.nhn?code={item_code}&page={page_no}"
    headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'}
    response = requests.get(url, headers=headers)
    html = bs(response.text, "lxml")
    table = html.select("table")
    table = pd.read_html(str(table))
    df_day = table[0].dropna()

    return df_day



def stock_crawl():
    value = '삼성전자'
    page = 3

    df = pd.DataFrame()
    for page in range(1, page):
        df = pd.concat([df,get_day_list(get_code(value),page)], ignore_index=True)
    
    # df.dropna()를 이용해 결측값 있는 행 제거
    # 전처리
    stock_df = df.dropna()

    print(stock_df)

    stock_df = stock_df.rename(columns= {'날짜': 'date', '종가': 'close', '전일비': 'diff','시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})
    stock_df[['close', 'open', 'high', 'low', 'volume']] = stock_df[['close','open', 'high', 'low', 'volume']].astype(int)

    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine('postgresql://postgres:postgres@192.168.168.133:30032/stock')
    # Replace 'table_name' with your desired table name
    stock_df.to_sql('tb_stock_dt', engine, if_exists='replace', index=False)


default_args={'start_date': days_ago(1)}

# DAG 틀 설정
with DAG(
    dag_id="stock-data-pull",
    # crontab 표현 사용 가능 https://crontab.guru/
    schedule_interval=None, 
    default_args=default_args,
    # 태그는 원하는대로
    tags=["stock", "data", "api", "postgresql", "pull"],
    # catchup을 True로 하면, start_date 부터 현재까지 못돌린 날들을 채운다
    catchup=False) as dag:



    creating_table = PostgresOperator(
        task_id="creating_table",
        postgres_conn_id='stock_test',
        # tb_stock_dt 라는 테이블이 없는 경우에만 만들도록 IF NOT EXISTS 조건을 넣어주자.
        sql="""
            CREATE TABLE IF NOT EXISTS tb_stock_dt( 
                date DATE,
                close INT,
                diff VARCHAR,
                open INT,
                high INT,
                low INT,
                volume INT
            );
        """,
    )



    stock_dt_pull = PythonOperator(
        task_id="stock_dt_pull",
        python_callable=stock_crawl, # 실행할 파이썬 함수
        provide_context=True,
        dag=dag,
    )

    # 대그 완료 출력
    print_complete = PythonOperator(
            task_id="print_complete",
            python_callable=_complete # 실행할 파이썬 함수
    )


    # 파이프라인 구성하기
    creating_table >> stock_dt_pull >> print_complete