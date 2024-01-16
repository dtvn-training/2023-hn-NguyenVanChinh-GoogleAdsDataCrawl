from datetime import datetime

def writeAirflowLog(logText):
    print(logText)
    
    airflowLogPath = "airflowLogs/{}_airflow.log".format(datetime.now().strftime("%Y%m%d"))
    with open(airflowLogPath, "a") as log_file:  # Open the log file in 'append' mode
        log_file.write(
            "INFO: {}\n".format(logText)
        )