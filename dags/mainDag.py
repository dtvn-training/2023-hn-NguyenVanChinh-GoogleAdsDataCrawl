import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from python.crawlWebData import executeCrawl
from python.transformData import transform
from python.loadToDB import loadToMySql
from python.checkLink import updateNewestLinkGoogleads
from python.crawlWebData import getLinkGoogleads
from python.endTask import writeLog
# from airflow_pentaho.operators.KitchenOperator import KitchenOperator

dag = DAG(
    dag_id="crawlDataGoogleadsJob",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=timedelta(days=2000),
    tags=["etl", "pentaho", "googleads"],
)

updateLink = PythonOperator(task_id="get_newest_link", python_callable=updateNewestLinkGoogleads, dag=dag)

@task.branch(task_id="check_link_googleads")
def do_branching():
    oldLink = getLinkGoogleads(getNewest=False)
    todayLink = getLinkGoogleads(getNewest=True)
    if oldLink == todayLink:
        return "end_job"
    else:
        return "crawl_data"

checkLink = do_branching()

crawlData = PythonOperator(task_id="crawl_data", python_callable=executeCrawl, dag=dag)
transformData = PythonOperator(task_id="transform_data", python_callable=transform, dag=dag)
# checkDiff = PythonOperator(task_id="check_for_changes", python_callable=) #TODO: 
# loadData = PythonOperator(task_id="load_data_toMysql", python_callable=loadToMySql, dag=dag)
endJob = PythonOperator(task_id='end_job', python_callable=writeLog) 
# updateLinkConfig = EmptyOperator(task_id='update_link_config')

updateLink >> checkLink >> [crawlData, endJob]
crawlData >> transformData
#>> loadData >> updateLinkConfig