from loadToDB import create_connection
from loadToDB import readProperties
from loadToDB import createTableMySql
from sqlalchemy import create_engine
import pandas as pd
import logging


def getConnection():
    connectionInfo = readProperties("config/db.properties")
    mysqlConnection = create_connection(connectionInfo)
    return mysqlConnection, connectionInfo


def loadTempTable(mysqlConnection, connectionInfo):
    createTableMySql(mysqlConnection, "config/createTmpTables.sql")
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}:3306/{db}".format(
            host=connectionInfo.get("host"),
            pw=connectionInfo.get("password"),
            user=connectionInfo.get("user"),
            db=connectionInfo.get("database"),
        )
    )

    # save and write log
    logging.basicConfig(
        # filename="log/loadtodb.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
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
        df.to_sql(
            tableName.lower() + "tmp", engine, if_exists="append", index=False
        )  # lower because tablename in mysql always in lowercase form
        logging.info("Load to table {} successfully!".format(tableName))


def checkResourceField(connection, logger):
    logger.info("---------Start check ResourceField table------------")

    cursor = connection.cursor()

    queryCount1 = "SELECT COUNT(*) as num_records FROM resourcefield"
    cursor.execute(queryCount1)
    result1 = cursor.fetchone()
    num_recordsSoFar = len(result1)

    queryCount2 = "SELECT COUNT(*) as num_records FROM resourcefieldtmp"
    cursor.execute(queryCount2)
    result2 = cursor.fetchone()
    num_recordsNew = len(result2)
    logger.info(
        "Number of record change from {} to {}, different: {}".format(
            num_recordsSoFar, num_recordsNew, abs(num_recordsNew - num_recordsSoFar)
        )
    )

    # check deleted field
    sql_scripts_checkResourceField1 = """SELECT  'resourcefield' AS `Table`, rf.FieldName, SUBSTRING(rf.FieldDescription, 1, 40) AS FieldDescription, 'Deleted' AS 'Status'
                                    FROM    resourcefield rf
                                    WHERE   rf.FieldName NOT IN
                                            (
                                            SELECT  FieldName
                                            FROM    resourcefieldtmp 
                                            )
                                    """
    # Execute the SQL script
    cursor.execute(sql_scripts_checkResourceField1)
    difResult1 = cursor.fetchall()
    logger.info(
        "Number of deleted field: {} ".format(len(difResult1))
    )
    for record in difResult1:
        logger.info(record)

    # check added field
    sql_scripts_checkResourceField2 = """SELECT  'resourcefield' AS `Table`, rft.FieldName, SUBSTRING(rft.FieldDescription, 1, 40) AS FieldDescription, 'New' AS 'Status'
                                    FROM    resourcefieldtmp rft
                                    WHERE   rft.FieldName NOT IN
                                            (
                                            SELECT  FieldName
                                            FROM    resourcefield
                                            )
                                    """
    # Execute the SQL script
    cursor.execute(sql_scripts_checkResourceField2)
    difResult2 = cursor.fetchall()
    logger.info(
        "Number of new field: {} ".format(len(difResult2))
    )
    for record in difResult2:
        logger.info(record)


def checkChanges(connection, logger):
    checkResourceField(connection, logger)


def dropTable(connection):
    cursor = connection.cursor()
    cursor.execute(
        "drop table if exists RelatedResourceTmp, ResourceTmp, ResourceFieldTmp, DataTypeTmp, SelectableWithTmp, ResourceFieldConnectTmp;"
    )
    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    mysqlConnection, connectionInfo = getConnection()
    loadTempTable(mysqlConnection, connectionInfo)

    # Create and configure logger
    logging.basicConfig(
        filename="log/checkChanges.log", format="%(asctime)s %(message)s", filemode="w"
    )
    logger = logging.getLogger()

    checkChanges(mysqlConnection, logger)

    # dropTable(mysqlConnection)
