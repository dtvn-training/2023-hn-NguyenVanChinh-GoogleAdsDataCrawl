from datetime import datetime
import subprocess
from python.loadToDB import readProperties
from selfLog import writeAirflowLog

def writeTransformLog():
    folderName = readProperties("config/foldername.properties").get("folder_name")

    logText = (
        "Transform data successfully! Result saved in folder: outputdata/{}".format(
            folderName
        )
    )
    writeAirflowLog(logText)


def transform():
    # Specify the path to your shell script
    script_path = "/home/chinhnv/2023-hn-NguyenVanChinh-GoogleadsApiDataCrawl/dags/python/transform.sh"

    # Execute the shell script
    subprocess.run(["sh", script_path])

    writeTransformLog()
