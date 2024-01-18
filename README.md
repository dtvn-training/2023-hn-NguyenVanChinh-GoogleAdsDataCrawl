# 2023-hn-NguyenVanChinh-GoogleAdsDataCrawl
### Overview:  
A project crawl data of GoogleAds report, and save to Database. And we can check for changes of any two versions in table.
- Crawl data: using Selenium and Python
- Transform data: using Pentaho (Kettle)
- Load data to Mysql: using Python
- Check for changes: using Python  
_The entire process is implemented on Airflow_  
Checking [here]('https://docs.google.com/presentation/d/1Z3wYE3DjlgQ2rXPdBs-1AbC0xepL2eZvpNMjddojobM/edit?usp=drive_link') for more details.

### Requirements:
- **Mysql**: (can config on file config/db.properties)  
- **Apache Airflow**: can deploy on local Linux or WSL
- **Google Chrome, chromedrive**: used for Selenium
- **Python package**: listed in _requirement.txt_

### How to deploy:
- clone code  
```shell
git clone https://github.com/dtvn-training/2023-hn-NguyenVanChinh-GoogleadsApiDataCrawl
```
- change airflow dags path:  
    Open file config: _airflow/airflow.cfg_  
    Add path of your dags folder to line:  _dags_folder=_  
    If OK, airflow webserver will show job **Crawl_GoogleadsData_Job**  
- Change path of kitchenHome and transformHone (to run PDI job) in file _transform.sh_
- Change script_path to file _transform.sh_ in _transformData.py_  
- Add chromeDriver: download from [website](https://googlechromelabs.github.io/chrome-for-testing/), unzip and move chromedrive to this respository    
**Note: Code has changed to can run success on Airflow, so some import function on _commonFunction.py_ in others python file can show error when run by python command**
