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

from bs4 import BeautifulSoup
import requests
import psycopg2
from datetime import timedelta
import pip
#from kafka import KafkaProducer
#from kafka import KafkaConsumer



def install_lib(package, upgrade=True):
    # package install with upgrade or not
    if hasattr(pip, 'main'):
        if upgrade:
            pip.main(['install', '--upgrade', package])
        else:
            pip.main(['install', package])
    else:
        if upgrade:
            pip._internal.main(['install', '--upgrade', package])
        else:
            pip._internal.main(['install', package])

    # import package
    try:
        eval(f"import {package}")
    except ModuleNotFoundError:
        print("# Package name might be differnt. please check it again.")
    except Exception as e:
        print(e)


def kafka_producer():
    # Kafka Producer 설정
    producer = KafkaProducer(bootstrap_servers=['192.168.168.133:31360', '192.168.168.133:32398', '192.168.168.133:30052'],
                            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('UTF-8'),
                            key_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('UTF-8'))

    # 웹 크롤링
    url = 'https://finance.naver.com/sise/sise_market_sum.naver'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    col_string = soup.select('#contentarea > div.box_type_l > table.type_2 > thead > tr')[0].text
    col = col_string.split('\n')[2:-2]

    data = []
    # 데이터 추출
    for i in range(len(col)):
        tmp = soup.select('#contentarea > div.box_type_l > table.type_2 > tbody > tr:nth-child(2) > td:nth-child({idx})'.format(idx=str(i+2)))[0].text.strip()
        data.append(tmp.replace('\t','').replace('\n',' '))
          
    col = [x.encode('utf-8') for x in col]

    mapping = {'종목명': 'Item',
    '현재가': 'Current',
    '전일비': 'diff',
    '등락률': 'fluctuation_rate',
    '액면가': 'face_value',
    '시가총액': 'market_capitalization',
    '상장주식수': 'Number_of_listed_shares',
    '외국인비율': 'the_proportion_of_foreigners',
    '거래량': 'volume',
    'PER': 'PER',
    'ROE': 'ROE'}

    col = list(mapping[x] for x in col)

    price_message = {k: v for k, v in zip(col, data)}
    price_message['date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(price_message)

    # 데이터를 Kafka 토픽으로 보내기
    producer.send('crawling-test', key='samsung_price', value=price_message)
    producer.flush()



def kafka_consumer():
    consumer = KafkaConsumer('crawling-test', 
                            bootstrap_servers=['192.168.168.133:31360', '192.168.168.133:32398', '192.168.168.133:30052'],
                            enable_auto_commit=True,
                            auto_offset_reset='earliest')
    conn = psycopg2.connect(dbname='stock', user='postgres', password='postgres', host='192.168.168.133', port=30032)
    cursor = conn.cursor()

    for message in consumer:
        print(message.value)
        m = eval(message.value)
        volume = int(m['volume'].replace(',', ''))
        Number_of_listed_shares = int(m['Number_of_listed_shares'].replace(',', ''))
        fluctuation_rate = m['fluctuation_rate']
        ROE = float(m['ROE'].replace(',', ''))
        PER = float(m['PER'].replace(',', ''))
        Current = int(m['Current'].replace(',', ''))
        Item = m['Item']
        the_proportion_of_foreigners = float(m['the_proportion_of_foreigners'].replace(',', ''))
        diff = m['diff']
        face_value = int(m['face_value'].replace(',', ''))
        market_capitalization = int(m['market_capitalization'].replace(',', ''))
        date = m['date']


        data = (Item, Current, diff, fluctuation_rate, face_value, market_capitalization, Number_of_listed_shares, the_proportion_of_foreigners, volume, PER, ROE, date)
        cursor.execute("INSERT INTO crawling_test VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data)
        print("insert 완료")
        cursor.execute("DELETE FROM crawling_test t1 WHERE EXISTS ( SELECT 1 FROM crawling_test t2 WHERE t1.date = t2.date AND t1.ctid > t2.ctid)")
        print("중복제거 완료")
        conn.commit()
    cursor.close()
    conn.close()

def _complete():
    print("주식 데이터 pull DAG 완료")


# 디폴트 설정
default_args={'start_date': days_ago(1)}

with DAG(
    dag_id="kafka-data-pull",
    # crontab 표현 사용 가능 https://crontab.guru/
    schedule_interval="@daily", 
    default_args=default_args,
    # 태그는 원하는대로
    tags=["kafka", "search", "local", "api", "pipeline"],
    # catchup을 True로 하면, start_date 부터 현재까지 못돌린 날들을 채운다
    catchup=False) as dag:


    creating_table = PostgresOperator(
    task_id="creating_table",
    postgres_conn_id='stock_test',
    # naver_search_result 라는 테이블이 없는 경우에만 만들도록 IF NOT EXISTS 조건을 넣어주자.
    sql="""
        CREATE TABLE IF NOT EXISTS crawling_test( 
            Item VARCHAR,
            Current FLOAT,
            diff VARCHAR,
            fluctuation_rate FLOAT,
            face_value FLOAT,
            market_capitalization FLOAT,
            Number_of_listed_shares FLOAT,
            the_proportion_of_foreigners FLOAT,
            volume FLOAT,
            PER FLOAT,
            ROE FLOAT,
            date DATE
        );
    """,
    )


    kafka_producer_pull = PythonOperator(
        task_id="kafka_producer_pull",
        python_callable=kafka_producer, # 실행할 파이썬 함수
        provide_context=True,
        dag=dag,
    )

    kafka_consumer_push = PythonOperator(
        task_id="kafka_consumer_push",
        python_callable=kafka_consumer, # 실행할 파이썬 함수
        provide_context=True,
        dag=dag,
    )

    # 대그 완료 출력
    print_complete = PythonOperator(
        task_id="print_complete",
        python_callable=_complete # 실행할 파이썬 함수
    )


    # 파이프라인 구성하기
    creating_table >> kafka_producer_pull >> kafka_consumer_push >> print_complete