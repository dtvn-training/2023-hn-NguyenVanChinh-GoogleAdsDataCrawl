import pandas as pd

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


if __name__ == "__main__":
    # read properties file
    folderName = readProperties("config/foldername.properties")
    outputdataPath = "outputdata/" + folderName.get("folder_name")

    # read properties mysql


    # read csv
    df = pd.read_csv(outputdataPath + '/Resource.csv', sep=';')

    print(df.to_string()) 

    # save and write log