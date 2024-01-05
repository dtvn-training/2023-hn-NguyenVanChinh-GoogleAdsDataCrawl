import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from operators.crawlData import crawData

with DAG(
    dag_id="python",
    start_date=datetime.datetime(2021, 1, 1),
     schedule_interval=timedelta(days=2000),
    tags=['crawl', 'v1', 'linux']
) as dag:
    hello_task = crawData(task_id="crawl-data")
    hello_task
