from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine

import json
from pandas import json_normalize
import pandas as pd
from datetime import datetime

#from sklearn.model_selection import train_test_split
#from sklearn.metrics import r2_score

#import statsmodels.api as sm
#from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
#from statsmodels.tsa.arima.model import ARIMA
#from statsmodels.tsa.statespace.sarimax import SARIMAX
#from pmdarima.arima import auto_arima

from datetime import datetime, timedelta
import itertools

import warnings
warnings.filterwarnings('ignore')

import os

import requests
from bs4 import BeautifulSoup as bs

def dag_function(**kwargs):
    import sys
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'packagename'])

    import packagename
    # ... Use package

def fetch_data_from_postgres(**context):
    # Initialize PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_stock')
    
    # Execute SQL query to fetch data
    sql_query = "SELECT * FROM  batch_test"
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_query)
    
    # Fetch all rows from the executed query
    rows = cursor.fetchall()
    
    # Get column names from the cursor description
    colnames = [desc[0] for desc in cursor.description]
    
    # Convert fetched data to DataFrame
    df = pd.DataFrame(rows, columns=colnames)
    
    # Convert DataFrame to JSON string
    df_json = df.to_json(orient='records')
    
    # Push JSON data to XCom
    context['task_instance'].xcom_push(key='dataframe_json', value=df_json)


def process_data_from_xcom(**context):
    # Get JSON data from XCom
    df_json = context['task_instance'].xcom_pull(task_ids='fetch_data_from_postgres', key='dataframe_json')
    
    # Convert JSON to DataFrame
    df_init = pd.read_json(df_json, orient='records')
    
    # Process the DataFrame as needed
    #print("DataFrame received from XCom:")
    #print(df)

    df = df_init.set_index(keys='date')
    print(df)

dag = DAG(
    'processing_forecast',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,
)

fetch_data = PythonOperator(
    task_id='fetch_data_from_postgres',
    python_callable=fetch_data_from_postgres,
    provide_context=True,
    dag=dag,
)

reprocess_data = PythonOperator(
    task_id='process_data_from_xcom',
    python_callable=process_data_from_xcom,
    provide_context=True,
    dag=dag,
)




fetch_data >> reprocess_data  # Set task dependencies





