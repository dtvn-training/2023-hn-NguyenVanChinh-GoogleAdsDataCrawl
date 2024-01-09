import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
# from operators.crawlData import crawData
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from python.crawlWebData import executeCrawl
from python.transformData import transform
# from airflow_pentaho.operators.KitchenOperator import KitchenOperator

with DAG(
    dag_id="crawlDataJob",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=timedelta(days=2000),
    tags=["etl", "v1", "googleads"],
) as dag:
    crawlData = PythonOperator(task_id='crawlData', python_callable=executeCrawl)
    transform = PythonOperator(task_id='transformData', python_callable=transform)
    crawlData >> transform