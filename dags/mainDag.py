import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from python.crawlWebData import executeCrawl
from python.transformData import transform
from python.loadToDB import loadToMySql

# from airflow_pentaho.operators.KitchenOperator import KitchenOperator

with DAG(
    dag_id="crawlDataGoogleadsJob",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=timedelta(days=2000),
    tags=["etl", "pentaho", "googleads"],
) as dag:
    crawlData = PythonOperator(task_id="crawl_data", python_callable=executeCrawl)
    transformData = PythonOperator(task_id="transform_data", python_callable=transform)
    loadData = PythonOperator(task_id="load_data_toMysql", python_callable=loadToMySql)
    crawlData >> transformData >> loadData
