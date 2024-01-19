from python.commonFunction import create_connection
from python.commonFunction import readProperties
from python.commonFunction import writeAirflowLog
from python.commonFunction import getLinkGoogleads
from python.commonFunction import getVersion
import pandas as pd


def getConnection():
    connectionInfo = readProperties("config/db.properties")
    mysqlConnection = create_connection(connectionInfo)
    return mysqlConnection

def countRows(cursor, tableName, Version):
    queryCount1 = "SELECT COUNT(*) as num_records FROM {} WHERE GoogleadsApiVersion = {};".format(tableName, Version)
    cursor.execute(queryCount1)
    result1 = cursor.fetchone()
    num_records = result1[0]
    return num_records

def writeDiffDataToLog(tableName, df, mode='Write'):
    outputdataPath = 'logs/{}/Diff{}.csv'.format(readProperties("config/foldername.properties").get("folder_name"), tableName)
    if mode == 'Append':
        df.to_csv(outputdataPath, index=False, sep=';', mode='a', header=False)
    else:
        df.to_csv(outputdataPath, index=False, sep=';')

def checkResourceField(connection, oldVersion, newVersion):
    writeAirflowLog("--Start check ResourceField table v{} vs v{}".format(oldVersion, newVersion))

    cursor = connection.cursor()
    num_recordsSoFar = countRows(cursor, 'ResourceField', oldVersion)
    num_recordsNew = countRows(cursor, 'ResourceField', newVersion)
    writeAirflowLog(
        "Number of record change from {} to {}, different: {}".format(
            num_recordsSoFar, num_recordsNew, abs(num_recordsNew - num_recordsSoFar)
        ),
    )

    sql_scripts_checkResourceField = """SELECT rf.FieldName, rf.FieldDescription, rf.Category, rf.GoogleadsApiVersion, '{}' AS 'Status'
                                    FROM    ResourceField rf
                                    WHERE   rf.GoogleadsApiVersion = {} 
										AND FieldName NOT IN
                                            (SELECT  FieldName
                                            FROM    ResourceField WHERE GoogleadsApiVersion = {}
                                            )
                                    """
    ResourceFieldColumns = ['FieldName', 'FieldDescription', 'Category', 'GoogleadsApiVersion', 'Status']
    
    # Execute the SQL script check deleted field
    cursor.execute(sql_scripts_checkResourceField.format('Deleted', oldVersion, newVersion))
    difResult1 = cursor.fetchall()
    writeAirflowLog("Number of deleted fields: {} ".format(len(difResult1)))
    df1 = pd.DataFrame(difResult1, columns=ResourceFieldColumns)
    writeDiffDataToLog('ResourceField', df1)


    # Execute the SQL script check added field
    cursor.execute(sql_scripts_checkResourceField.format('New', newVersion, oldVersion))
    difResult2 = cursor.fetchall()
    writeAirflowLog("Number of new fields: {} ".format(len(difResult2)))
    df2 = pd.DataFrame(difResult2, columns=ResourceFieldColumns)
    writeDiffDataToLog('ResourceField', df2, mode='Append')

def checkDataType(connection, oldVersion, newVersion):
    writeAirflowLog("--Start check Datatype table v{} vs v{}".format(oldVersion, newVersion))
    cursor = connection.cursor()

    countOld = countRows(cursor, 'DataType', oldVersion)
    countNew = countRows(cursor, 'DataType', newVersion)
    writeAirflowLog(
        "Number of record change from {} to {}, different: {}".format(
            countOld, countNew, abs(countOld - countNew)
        )
    )

    # check deleted field
    sql_scripts_checkDataType = """SELECT rf.FieldName, dt.DataType, dt.EnumDataType, dt.GoogleadsApiVersion, '{}' as Status
            FROM ResourceField rf JOIN DataType dt ON rf.FieldId = dt.FieldId
            WHERE dt.GoogleadsApiVersion = {} AND ROW(rf.FieldName, dt.DataType, dt.EnumDataType) NOT IN 
	        (SELECT rf.FieldName, dt.DataType, dt.EnumDataType FROM ResourceField rf JOIN DataType dt ON rf.FieldId = dt.FieldId WHERE dt.GoogleadsApiVersion = {})
                                    """
    DataTypeColumns = ['FieldName', 'DataType', 'EnumDataType', 'GoogleadsApiVersion', 'Status']
    # Execute the SQL script
    cursor.execute(sql_scripts_checkDataType.format('Deleted', oldVersion, newVersion))
    difResult1 = cursor.fetchall()
    writeAirflowLog("Number of deleted fields: {} ".format(len(difResult1)))
    df1 = pd.DataFrame(difResult1, columns=DataTypeColumns)
    writeDiffDataToLog('DataType', df1)
    
    
    # Execute the SQL script
    cursor.execute(sql_scripts_checkDataType.format('New', newVersion, oldVersion))
    difResult2 = cursor.fetchall()
    writeAirflowLog("Number of new records: {} ".format(len(difResult2)))
    df2 = pd.DataFrame(difResult2, columns=DataTypeColumns)
    writeDiffDataToLog('DataType', df2, mode='Append')

def checkResource(connection, oldVersion, newVersion):
    writeAirflowLog("--Start check Resources table v{} vs v{}".format(oldVersion, newVersion))

    cursor = connection.cursor()

    countOld = countRows(cursor, 'Resources', oldVersion)
    countNew = countRows(cursor, 'Resources', newVersion)
    writeAirflowLog(
        "Number of record change from {} to {}, different: {}".format(
            countOld, countNew, abs(countOld - countNew)
        ),
    )

    sql_scripts_checkResources = """SELECT r.ResourceName, r.ResourceDescription, r.ResourceWithMetric, r.GoogleadsApiVersion, '{}' AS 'Status'
                                    FROM Resources r
                                    WHERE r.GoogleadsApiVersion = {} AND r.ResourceName NOT IN (SELECT ResourceName FROM Resources WHERE GoogleadsApiVersion = {});
                                    """
    ResourcesColumns = ['ResourceName', 'ResourceDescription', 'ResourceWithMetric', 'GoogleadsApiVersion', 'Status']
                                    
    # Execute the SQL script check deleted field
    cursor.execute(sql_scripts_checkResources.format('Deleted', oldVersion, newVersion))
    difResult1 = cursor.fetchall()
    writeAirflowLog("Number of deleted records: {} ".format(len(difResult1)))
    df1 = pd.DataFrame(difResult1, columns=ResourcesColumns)
    writeDiffDataToLog('Resources', df1)

    # Execute the SQL script check added field
    cursor.execute(sql_scripts_checkResources.format('New', newVersion, oldVersion))
    difResult2 = cursor.fetchall()
    writeAirflowLog("Number of new records: {} ".format(len(difResult2)))
    df2 = pd.DataFrame(difResult2, columns=ResourcesColumns)
    writeDiffDataToLog('Resources', df2, mode='Append')

def checkChanges():
    connection = getConnection()
    oldVersion = getVersion(getLinkGoogleads(getNewest=False))
    newVersion = getVersion(getLinkGoogleads(getNewest=True))
    
    checkResourceField(connection, oldVersion, newVersion)
    checkDataType(connection, oldVersion, newVersion)
    checkResource(connection, oldVersion, newVersion)

def updateLinkGoogleads():
    file_path = "config/googleadsLink.properties"
    with open(file_path, "w") as file:
        file.write("googleadsLink={}".format(getLinkGoogleads(getNewest=True)))

def checkDifferences():
    writeAirflowLog('------- Start checkForChanges.py ---------')

    # if crawled googleadsLink is null, data googleads is empty. So dont need check
    if getLinkGoogleads(getNewest=False) is not None:
        checkChanges()
    else:
        writeAirflowLog('Dont have old table to compare')

    updateLinkGoogleads()
    