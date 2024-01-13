import logging
from python.crawlWebData import getLinkGoogleads
from datetime import datetime


def writeLog():
    logText = "Link not change. New link still is: {}".format(
        getLinkGoogleads(getNewest=True)
    )
    print(logText)

    log_file = open("logs/resultAirflow.log", "a")  # Open the log file in 'append' mode
    log_file.write(
        "{} INFO: {}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), logText)
    )
    log_file.close()
