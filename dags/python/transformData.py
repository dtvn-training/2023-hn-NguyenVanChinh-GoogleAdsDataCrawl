from datetime import datetime
import pandas as pd
import subprocess
from python.commonFunction import readProperties
from python.commonFunction import writeAirflowLog
from python.commonFunction import create_connection
from python.commonFunction import createTableMySql

def writeTransformLog():
    folderName = readProperties("config/foldername.properties").get("folder_name")

    logText = (
        "Transform data successfully! Result saved in folder: outputdata/{}".format(
            folderName
        )
    )
    writeAirflowLog(logText)

'''
because when transform by PDI, id of ResourceField, Resources was created begin
from 0 to merge and create ResourceFieldConnect, RelatedResource table
But if write data version 13, 14, 15 in same table, index must be incorrect.
It cant be auto_increment because merge before write to table.
So I calculate new id and write to file. If write to table will as auto increment
'''
def fixIndexTable():
    # count number of records in mysql
    connectionInfo = readProperties("config/db.properties")

    connection = create_connection(connectionInfo)
    cursor = connection.cursor()

    queryCount1 = "SELECT COUNT(*) as num_records FROM ResourceField"
    cursor.execute(queryCount1)
    result1 = cursor.fetchone()
    num_resourceFieldsSoFar = result1[0]
    
    queryCount2 = "SELECT COUNT(*) as num_records FROM Resources"
    cursor.execute(queryCount2)
    result2 = cursor.fetchone()
    num_resourcesSoFar = result2[0]
    
    # read folder name
    folderName = readProperties("config/foldername.properties")
    outputdataPath = "outputdata/" + folderName.get("folder_name") + "/"
    
    # modify id and write to its csv file
    df1 = pd.read_csv(outputdataPath + 'ResourceField' + ".csv", sep=";")
    df1['FieldId'] += num_resourceFieldsSoFar + 1
    df1.to_csv(outputdataPath + 'ResourceField.csv', index=False, sep=';')
    
    df2 = pd.read_csv(outputdataPath + 'DataType' + ".csv", sep=";")
    df2['FieldId'] += num_resourceFieldsSoFar + 1
    df2.to_csv(outputdataPath + 'DataType.csv', index=False, sep=';')
    
    df3 = pd.read_csv(outputdataPath + 'SelectableWith' + ".csv", sep=";")
    df3['FieldId'] += num_resourceFieldsSoFar + 1
    df3.to_csv(outputdataPath + 'SelectableWith.csv', index=False, sep=';')
    
    df4 = pd.read_csv(outputdataPath + 'Resources' + ".csv", sep=";")
    df4['ResourceId'] += num_resourcesSoFar + 1
    df4.to_csv(outputdataPath + 'Resources.csv', index=False, sep=';')
    
    df5 = pd.read_csv(outputdataPath + 'RelatedResource' + ".csv", sep=";")
    df5['MasterResourceId'] += num_resourcesSoFar + 1
    df5['AttributedResourceId'] += num_resourcesSoFar + 1
    df5.to_csv(outputdataPath + 'RelatedResource.csv', index=False, sep=';')
    
    df6 = pd.read_csv(outputdataPath + 'ResourceFieldConnect' + ".csv", sep=";")
    df6['FieldId'] += num_resourceFieldsSoFar + 1
    df6['ResourceId'] += num_resourcesSoFar + 1
    df6.to_csv(outputdataPath + 'ResourceFieldConnect.csv', index=False, sep=';')

def transform():
    
    # read connection to mysql
    connectionInfo = readProperties("config/db.properties")

    # prepare db by create if not exists table.
    mysqlConnection = create_connection(connectionInfo)
    createTableMySql(mysqlConnection, "config/createtables.sql")
    
    
    # Specify the path to your shell script
    script_path = "/home/chinhnv/2023-hn-NguyenVanChinh-GoogleadsApiDataCrawl/dags/python/transform.sh"

    # Execute the shell script
    subprocess.run(["sh", script_path])
    fixIndexTable()
    writeTransformLog()