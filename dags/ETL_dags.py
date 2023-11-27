from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'chanmin.cho',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('etl_pipeline', default_args=default_args, schedule_interval='@daily')

def extract():
    df = pd.read_csv('/opt/airflow/datalake/hospitals.csv')
    return df

def transform(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract')

    cost_columns = [col for col in df.columns if 'Cost' in col]
    quality_columns = [col for col in df.columns if 'Quality' in col]

    df['Average_Cost'] = df[cost_columns].mean(axis=1)
    df['Average_Quality'] = df[quality_columns].apply(lambda x: x.map({'Worse': 1, 'Average': 2, 'Higher': 3}).mean(), axis=1)


    ti.xcom_push(key='transformed_data', value=df)


def load(**kwargs):
    ti = kwargs['ti']
    transformed_df = ti.xcom_pull(key='transformed_data')
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    transformed_df.to_sql('transformed_data', con=engine, if_exists='replace')

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task
