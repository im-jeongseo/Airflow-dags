from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine
import pandas as pd
from pandas import json_normalize
import numpy as np

from datetime import datetime, timedelta
import itertools

import warnings
warnings.filterwarnings('ignore')
import os


def fetch_data_from_postgres(**kwargs):
    # Initialize PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_stock')
    
    # Execute SQL query to fetch data
    sql_query = "SELECT * FROM  batch"
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_query)
    
    # Fetch all rows from the executed query
    rows = cursor.fetchall()
    # Get column names from the cursor description
    colnames = [desc[0] for desc in cursor.description]
    
    # Convert fetched data to DataFrame
    df = pd.DataFrame(rows, columns=colnames)
    print(df)
    
    # Convert DataFrame to JSON string
    #df_json = df.to_json(orient='records')
    df_dict = df.to_dict(orient='records')
    
    # Push JSON data to XCom
    kwargs['ti'].xcom_push(key='dataframe', value=df_dict)


#def process_data_from_xcom(task_instance, **kwargs):
def process_data_from_xcom(**kwargs):
    #from sklearn.model_selection import train_test_split
    
    #import statsmodels.api as sm
    #from statsmodels.tsa.arima.model import ARIMA
    #from statsmodels.tsa.statespace.sarimax import SARIMAX
    
    # xcom으로 postgres table pull
    import pandas as pd
    from airflow.models import TaskInstance

    # df_dict = task_instance.xcom_pull(task_ids='fetch_data_from_postgres', key='dataframe')
    # df_init = pd.DataFrame(df_dict)
    # print(df_init)

    # Initialize Airflow configuration


    # Configure logging

    #@provide_session
    #def get_xcom_value(task_instance_key_str, session=None):
    #   ti = TaskInstance(task_instance_key_str, session=session)
    #   return ti.xcom_pull(task_ids='fetch_data_from_postgres', key='dataframe')
    
    #task_instance = kwargs['task_instance']
    #execution_date = kwargs['execution_date']

    # task_instance_dict = {
    #     'dag_id': 'processing_forecast',
    #     'task_id': 'fetch_data_from_postgres',
    #     'execution_date': execution_date,
    # }

    #print(df_init)

    #df_dict = get_xcom_value(task_instance_dict)
    
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='fetch_data_from_postgres', key='dataframe')
    df = pd.DataFrame(df_dict)
    print(df)

    ## Get JSON data from XCom
    #df_json = context['task_instance'].xcom_pull(task_ids='fetch_data_from_postgres', key='dataframe_json')
    
    ## Convert JSON to DataFrame
    #df_init = pd.read_json(df_json, orient='records')
    
    #df = df_init.set_index(keys='date')
    #print(df)
    
    # train,test split
    #train_data, test_data = train_test_split(df, test_size=0.3, shuffle=False)
    #dt=datetime.datetime.today()
    
    # 예측을 위한 date index 생성

    # import datetime
    # dt=datetime.datetime.today()

    # data_idx=[]
    # for i in range(10):
    #     delta = datetime.timedelta(days = i)
    #     dtnew = dt + delta
    #     data_idx.append(str(dtnew))


    # ARIMA 모델 생성
    # p = range(0, 2)
    # d = range(1, 3)
    # q = range(0, 2)
    # pdq = list(itertools.product(p, d, q))

    # AIC = []
    # for i in pdq :
    #     model = ARIMA(train_data['Close'].values, order=(i))
    #     model_fit = model.fit()
    #     print(f'ARIMA pdq : {i} >> AIC : {round(model_fit.aic, 2)}')
    #     AIC.append(round(model_fit.aic, 2))

    # optim = [(pdq[i], j) for i, j in enumerate(AIC) if j == min(AIC)]
    # print(optim)

    # model = ARIMA(train_data['Close'].values, order=optim[0][0])
    # model_fit = model.fit()    

    # # 예측값 생성
    # pred = model_fit.get_forecast(len(test_data) +10)
    # pred_val = pred.predicted_mean

    # pred_index = list(test_data.index)
    # for i in date_idx:
    #     pred_index.append(i)
    
    # print(pred_val)


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

reprocess_data = PythonVirtualenvOperator(
    task_id='process_data_from_xcom',
    python_callable=process_data_from_xcom,
    #requirements=["scikit-learn","statsmodels","apache-airflow"],
    requirements=["pickle","pandas","apache-airflow"], # 가상환경에서 필요한 모든 패키지가 명시
    system_site_packages=False, # Airflow가 설치된 시스템 사이트 패키지를 사용하도록 설정
    provide_context=True, # To pass the context variables
    #op_args=['{{ task_instance_key_str }}', '{{ execution_date }}'],
    dag=dag,
)




fetch_data >> reprocess_data  # Set task dependencies





