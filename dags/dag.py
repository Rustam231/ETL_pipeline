from sqlalchemy import create_engine
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests


def extract(ti):
    url = "https://api.spacexdata.com/v4/rockets"
    request_API = requests.get(url)

    ti.xcom_push(key='json_file',value = request_API.json())


def transform(ti):
    data = ti.xcom_pull(task_ids = 'extract',key = 'json_file')
    df = pd.json_normalize(data)

    #First table
    df1 = df.filter(items=["name", "active", "stages"])
    df1.index = df1.index + 1

    #Second table
    df2 = df[["mass.kg", "diameter.meters", "height.meters", "cost_per_launch"]]
    df2.index = df2.index + 1

    #Third table
    df3 = df.filter(
        items=[
            "first_stage.fuel_amount_tons",
            "first_stage.burn_time_sec",
            "first_stage.engines",
            "first_stage.engines",
            "first_stage.reusable",
            "second_stage.fuel_amount_tons",
            "second_stage.burn_time_sec",
            "second_stage.engines",
            "second_stage.engines",
            "second_stage.reusable",
        ],
        axis=1,
    )
    df3.index = df3.index + 1

    df1.to_csv('/opt/airflow/dags/output.csv', index=True)
    df2.to_csv('/opt/airflow/dags/charachteristics.csv')
    df3.to_csv('/opt/airflow/dags/result.csv')

    tables = [df1,df2,df3]
    ti.xcom_push(key='table',value = tables)


default_args = {
    'owner':'rustam',
    'retries':5,
    'retry_delay':timedelta(minutes=0.5)
}


with DAG(
    default_args=default_args,
    dag_id = 'spaceX_project',
    description = 'ETL pipeline - spacex project',
    start_date = datetime(2024,7,9),
    schedule_interval = '@daily',
    catchup = False
)as dag:

    task_extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract
    )

    task_transform = PythonOperator(
        task_id = 'transform',
        python_callable = transform
    )

    task_create_charachteristics = PostgresOperator(
        task_id = 'create_charachteristics',
        postgres_conn_id = 'cosmos',
        sql = """
            CREATE TABLE IF NOT EXISTS charachteristics(
                id INT PRIMARY KEY,
                mass INT,
                diameter FLOAT,
                height FLOAT,
                cost_per_launch INT
            );
        """,
    )

    task_create_result = PostgresOperator(
        task_id = 'create_result',
        postgres_conn_id = 'cosmos',
        sql = """
            CREATE TABLE IF NOT EXISTS result(
                id INT PRIMARY KEY,
                name TEXT,
                active BOOLEAN
            );
        """,
    )

    task_create_output = PostgresOperator(
        task_id='create_output',
        postgres_conn_id='cosmos',
        sql = """
            CREATE TABLE IF NOT EXISTS output(
                id INT PRIMARY KEY,
                name TEXT,
                active BOOLEAN,
                stages INT
            );
        """,
    )

    task_load_charachteristics = PostgresOperator(
        task_id = 'load_charachteristics',
        postgres_conn_id = 'cosmos',
        sql = """
            COPY charachteristics
            FROM 'C:\Users\User\Downloads\SpaceX project\dags\charachteristics.csv'
            DELIMITER ','
            CSV HEADER;
        """,
    )

    task_load_result = PostgresOperator(
        task_id = 'load_result',
        postgres_conn_id = 'cosmos',
        sql = """
            COPY result
            FROM 'C:\Users\User\Downloads\SpaceX project\dags\result.csv'
            DELIMITER ','
            CSV HEADER;
        """,
    )

    task_load_output = PostgresOperator(
        task_id = 'load_output',
        postgres_conn_id = 'cosmos',
        sql = """
            COPY output
            FROM 'C:\Users\User\Downloads\SpaceX project\dags\output.csv'
            DELIMITER ','
            CSV HEADER;
        """,
    )

    task_extract >> task_transform
    task_transform >> task_create_charachteristics >> task_load_charachteristics
    task_transform >> task_create_result >> task_load_result
    task_transform >> task_create_output >> task_load_output
