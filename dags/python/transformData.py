from datetime import datetime
import subprocess
from python.loadToDB import readProperties


def writeTransformLog():
    folderName = readProperties("config/foldername.properties").get("folder_name")

    logText = (
        "Transform data successfully! Result saved in folder: outputdata/{}".format(
            folderName
        )
    )
    print(logText)

    log_file = open("logs/resultAirflow.log", "a")  # Open the log file in 'append' mode
    log_file.write(
        "{} INFO: {}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), logText)
    )
    log_file.close()


def transform():
    # Specify the path to your shell script
    script_path = "/home/chinhnv/2023-hn-NguyenVanChinh-GoogleadsApiDataCrawl/dags/python/transform.sh"

    # Execute the shell script
    subprocess.run(["sh", script_path])

    writeTransformLog()
