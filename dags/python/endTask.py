from python.crawlWebData import getLinkGoogleads
from python.selfLog import writeAirflowLog

def handleEndJob():
    logText = "Link not change. New link still is: {}".format(
        getLinkGoogleads(getNewest=True)
    )
    writeAirflowLog(logText)