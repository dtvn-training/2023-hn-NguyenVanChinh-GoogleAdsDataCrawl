from datetime import datetime

def writeAirflowLog(logText):
    print(logText)
    
    airflowLogPath = "airflowLogs/{}_airflow.log".format(datetime.now().strftime("%Y%m%d"))
    log_file = open(airflowLogPath, "a")  # Open the log file in 'append' mode
    log_file.write(
        "INFO: {}\n".format(logText)
    )
    log_file.close()