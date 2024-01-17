from python.commonFunction import getLinkGoogleads
from python.commonFunction import writeAirflowLog

def handleEndJob():
    logText = "Link not change. New link still is: {}".format(
        getLinkGoogleads(getNewest=True)
    )
    writeAirflowLog(logText)