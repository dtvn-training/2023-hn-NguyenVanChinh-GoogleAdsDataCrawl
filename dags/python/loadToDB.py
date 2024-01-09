import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import logging


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


def create_connection(connectionInfo):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=connectionInfo.get("host"),
            user=connectionInfo.get("user"),
            password=connectionInfo.get("password"),
            database=connectionInfo.get("database"),
        )
        print("Connected to MySQL database")
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")

    return connection


def createTableMySql(connection):
    # Create a cursor object to execute SQL statements
    cursor = connection.cursor()

    # Read the SQL script from the file
    with open("config/createdb.sql", "r") as file:
        sql_queries = file.read()
    
    sql_scripts = sql_queries.split(';')

    # Execute the SQL script
    for sql_script in sql_scripts:
        cursor.execute(sql_script)

    # Commit change
    connection.commit()

    # Close the cursor and connection
    cursor.close()
    connection.close()

# use all above functions
def loadToMySql():
    # read properties file
    folderName = readProperties("config/foldername.properties")
    outputdataPath = "outputdata/" + folderName.get("folder_name") + "/"
    # read connection to mysql
    connectionInfo = readProperties("config/db.properties")

    # prepare db by drop exist table and create new table.
    mysqlConnection = create_connection(connectionInfo)
    
    createTableMySql(mysqlConnection)

    # declare engine for save data
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
    for tableName in tableNames:
        df = pd.read_csv(outputdataPath + tableName + ".csv", sep=";")
        df.to_sql(tableName.lower(), engine, if_exists="append", index=False) # lower because tablename in mysql always in lowercase form
        logging.info("Load to table {} successfully!".format(tableName))
