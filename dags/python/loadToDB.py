import pandas as pd
from sqlalchemy import create_engine
from python.commonFunction import writeAirflowLog
from python.commonFunction import readProperties
from python.commonFunction import getLinkGoogleads
from python.commonFunction import getVersion

def updateLinkGoogleads():
    file_path = "config/googleadsLink.properties"
    with open(file_path, "w") as file:
        file.write("googleadsLink={}".format(getLinkGoogleads(getNewest=True)))

# use all above functions
def loadToMySql():
    # read properties file
    folderName = readProperties("config/foldername.properties")
    outputdataPath = "outputdata/" + folderName.get("folder_name") + "/"
    # read connection to mysql
    connectionInfo = readProperties("config/db.properties")

    # declare engine for save data
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}:3306/{db}".format(
            host=connectionInfo.get("host"),
            pw=connectionInfo.get("password"),
            user=connectionInfo.get("user"),
            db=connectionInfo.get("database"),
        )
    )

    # table name for save
    tableNames = [
        "ResourceField",
        "DataType",
        "SelectableWith",
        "Resources",
        "ResourceFieldConnect",
        "RelatedResource",
    ]
    for tableName in tableNames:
        df = pd.read_csv(outputdataPath + tableName + ".csv", sep=";")
        df['GoogleadsApiVersion'] = getVersion(getLinkGoogleads(getNewest=True))
        df.to_sql(
            tableName, engine, if_exists="append", index=False
        )
        writeAirflowLog("Load to table {} successfully!".format(tableName))
        
    updateLinkGoogleads()