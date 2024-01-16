from datetime import datetime
import re

def writeAirflowLog(logText):
    print(logText)
    
    airflowLogPath = "airflowLogs/{}_airflow.log".format(datetime.now().strftime("%Y%m%d"))
    with open(airflowLogPath, "a") as log_file:  # Open the log file in 'append' mode
        log_file.write(
            "INFO: {}\n".format(logText)
        )

# open file properties get link to googleads website
def readProperties(file_path):
    properties = {}
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=")
                properties[key.strip()] = value.strip().strip("'")
    return properties

# open file properties get link to googleads website
def getLinkGoogleads(getNewest=False):
    if getNewest:
        file_path = "config/newestLink.properties"
    else:
        file_path = "config/googleadsLink.properties"
    properties = {}
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=")
                properties[key.strip()] = value.strip().strip("'")
    return properties.get("googleadsLink")

# using regex get googleads api version
def getVersion(link):
    match = re.search(r'/v(\d+)/', link)

    if match:
        version = match.group(1)
        return version
    else:
        return -1