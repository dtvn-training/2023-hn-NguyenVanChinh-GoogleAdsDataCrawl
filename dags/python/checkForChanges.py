from datetime import datetime
from loadToDB import create_connection
from loadToDB import readProperties
from loadToDB import createTableMySql
from sqlalchemy import create_engine
import pandas as pd


def getConnection():
    connectionInfo = readProperties("config/db.properties")
    mysqlConnection = create_connection(connectionInfo)
    return mysqlConnection, connectionInfo


def loadTempTable(mysqlConnection, connectionInfo, logger):
    createTableMySql(mysqlConnection, "config/createTmpTables.sql")
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
        "Resource",
        "ResourceFieldConnect",
        "RelatedResource",
    ]

    folderName = readProperties("config/foldername.properties")
    outputdataPath = "outputdata/" + folderName.get("folder_name") + "/"
    for tableName in tableNames:
        df = pd.read_csv(outputdataPath + tableName + ".csv", sep=";")
        df.to_sql(tableName + "Tmp", engine, if_exists="append", index=False)
        selfLog(logger, "Load to table {}Tmp successfully!".format(tableName))


def checkResourceField(connection, logger):
    selfLog(logger, "-----Start check ResourceField table")

    cursor = connection.cursor()

    queryCount1 = "SELECT COUNT(*) as num_records FROM ResourceField"
    cursor.execute(queryCount1)
    result1 = cursor.fetchone()
    num_recordsSoFar = result1[0]

    queryCount2 = "SELECT COUNT(*) as num_records FROM ResourceFieldTmp"
    cursor.execute(queryCount2)
    result2 = cursor.fetchone()
    num_recordsNew = result2[0]
    selfLog(
        logger,
        "Number of record change from {} to {}, different: {}".format(
            num_recordsSoFar, num_recordsNew, abs(num_recordsNew - num_recordsSoFar)
        ),
    )

    # check deleted field
    sql_scripts_checkResourceField1 = """SELECT rf.FieldName, SUBSTRING(rf.FieldDescription, 1, 40) AS FieldDescription, rf.Category, 'Deleted' AS 'Status'
                                    FROM    ResourceField rf
                                    WHERE   rf.FieldName NOT IN
                                            (
                                            SELECT  FieldName
                                            FROM    ResourceFieldTmp 
                                            )
                                    """
    # Execute the SQL script
    cursor.execute(sql_scripts_checkResourceField1)
    difResult1 = cursor.fetchall()
    selfLog(logger, "Number of deleted fields: {} ".format(len(difResult1)))
    for record in difResult1:
        selfLog(logger, record)

    # check added field
    sql_scripts_checkResourceField2 = """SELECT rft.FieldName, SUBSTRING(rft.FieldDescription, 1, 40) AS FieldDescription, rft.Category, 'New' AS 'Status'
                                    FROM    ResourceFieldTmp rft
                                    WHERE   rft.FieldName NOT IN
                                            (
                                            SELECT  FieldName
                                            FROM    ResourceField
                                            )
                                    """
    # Execute the SQL script
    cursor.execute(sql_scripts_checkResourceField2)
    difResult2 = cursor.fetchall()
    selfLog(logger, "Number of new fields: {} ".format(len(difResult2)))
    for record in difResult2:
        selfLog(logger, record)

def checkDataType(connection, logger):
    selfLog(logger, "-----Start check Datatype table")

    cursor = connection.cursor()

    queryCount1 = "SELECT COUNT(*) as num_records FROM DataType"
    cursor.execute(queryCount1)
    result1 = cursor.fetchone()
    num_dataTypeSoFar = result1[0]

    queryCount2 = "SELECT COUNT(*) as num_records FROM DataTypeTmp"
    cursor.execute(queryCount2)
    result2 = cursor.fetchone()
    num_dataTypeNew = result2[0]
    selfLog(
        logger,
        "Number of record change from {} to {}, different: {}".format(
            num_dataTypeSoFar, num_dataTypeNew, abs(num_dataTypeSoFar - num_dataTypeNew)
        ),
    )

    # check deleted field
    sql_scripts_checkDataType1 = """SELECT rf.FieldName, dt.DataType, dt.EnumDataType, 'Deleted' as Status
                    FROM ResourceField rf JOIN DataType dt ON rf.FieldId = dt.FieldId
                    WHERE ROW(rf.FieldName, dt.DataType, dt.EnumDataType) NOT IN (SELECT rft.FieldName, dtt.DataType, dtt.EnumDataType
                    FROM ResourceFieldTmp rft JOIN DataTypeTmp dtt ON rft.FieldId = dtt.FieldId)
                                    """
    # Execute the SQL script
    cursor.execute(sql_scripts_checkDataType1)
    difResult1 = cursor.fetchall()
    selfLog(logger, "Number of deleted records: {} ".format(len(difResult1)))
    for record in difResult1:
        selfLog(logger, record)

    # check added field
    sql_scripts_checkDataType2 = """SELECT rft.FieldName, dtt.DataType, dtt.EnumDataType, 'New' as Status
                    FROM ResourceFieldTmp rft JOIN DataTypeTmp dtt ON rft.FieldId = dtt.FieldId
                    WHERE ROW(rft.FieldName, dtt.DataType, dtt.EnumDataType) NOT IN (SELECT rf.FieldName, dt.DataType, dt.EnumDataType
                    FROM ResourceField rf JOIN DataType dt ON rf.FieldId = dt.FieldId)
                                    """
    # Execute the SQL script
    cursor.execute(sql_scripts_checkDataType2)
    difResult2 = cursor.fetchall()
    selfLog(logger, "Number of new records: {} ".format(len(difResult2)))
    for record in difResult2:
        selfLog(logger, record)

def checkResource(connection, logger):
    selfLog(logger, "-----Start check Resources table")

    cursor = connection.cursor()

    queryCount1 = "SELECT COUNT(*) as num_records FROM Resource"
    cursor.execute(queryCount1)
    result1 = cursor.fetchone()
    num_resourceSoFar = result1[0]

    queryCount2 = "SELECT COUNT(*) as num_records FROM ResourceTmp"
    cursor.execute(queryCount2)
    result2 = cursor.fetchone()
    num_resourceNew = result2[0]
    selfLog(
        logger,
        "Number of record change from {} to {}, different: {}".format(
            num_resourceSoFar, num_resourceNew, abs(num_resourceSoFar - num_resourceNew)
        ),
    )

    # check deleted field
    sql_scripts_checkResource1 = """SELECT r.ResourceName, SUBSTRING(r.ResourceDescription, 1, 40) AS ResourceDescription, r.ResourceWithMetric, 'Deleted' AS 'Status'
                                    FROM    Resource r
                                    WHERE   r.ResourceName NOT IN (SELECT rt.ResourceName FROM ResourceTmp rt )
                                    """
    # Execute the SQL script
    cursor.execute(sql_scripts_checkResource1)
    difResult1 = cursor.fetchall()
    selfLog(logger, "Number of deleted records: {} ".format(len(difResult1)))
    for record in difResult1:
        selfLog(logger, record)

    # check added field
    sql_scripts_checkResource2 = """SELECT rt.ResourceName, SUBSTRING(rt.ResourceDescription, 1, 40) AS ResourceDescription, rt.ResourceWithMetric, 'New' AS 'Status'
                                    FROM    ResourceTmp rt
                                    WHERE   rt.ResourceName NOT IN (SELECT r.ResourceName FROM Resource r )
                                    """
    # Execute the SQL script
    cursor.execute(sql_scripts_checkResource2)
    difResult2 = cursor.fetchall()
    selfLog(logger, "Number of new records: {} ".format(len(difResult2)))
    for record in difResult2:
        selfLog(logger, record)

def checkChanges(connection, logger):
    # checkResourceField(connection, logger)
    # checkDataType(connection, logger)
    checkResource(connection, logger)


def dropTable(connection):
    cursor = connection.cursor()
    cursor.execute(
        "drop table if exists RelatedResourceTmp, ResourceTmp, ResourceFieldTmp, DataTypeTmp, SelectableWithTmp, ResourceFieldConnectTmp;"
    )
    connection.commit()
    cursor.close()
    connection.close()


def selfLog(logger, logText):
    print(logText)
    logger.write("{} INFO: {}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), logText))


if __name__ == "__main__":
    logger = open("logs/resultAirflow.log", "a")
    mysqlConnection, connectionInfo = getConnection()
    # loadTempTable(mysqlConnection, connectionInfo, logger)
    
    # TODO: if link so far null -> dont check
    checkChanges(mysqlConnection, logger)

    # dropTable(mysqlConnection)
