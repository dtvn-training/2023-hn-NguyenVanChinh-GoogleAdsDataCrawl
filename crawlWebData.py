import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import shutil


# send request get html raw data by link
def getHtmlData(googleadsLink):
    googleadsResponse = requests.get(googleadsLink)  # get response
    if googleadsResponse.status_code == 200:  # if request ok, return html text
        return googleadsResponse.text
    else:  # if not ok, return status code
        return "error, code: {}".format(googleadsResponse.status_code)

# from htmlRawData, extract necessary info of resources
def extractInfoResource(googleadsRawData):
    soupGoogleadsData = BeautifulSoup(googleadsRawData, "html.parser")
    GoogleadsV15Data = soupGoogleadsData.select_one(
        "div.devsite-mobile-nav-bottom ul.devsite-nav-section li.devsite-nav-item.devsite-nav-expandable"
    )
    ResourceRawData = GoogleadsV15Data.select(
        "ul.devsite-nav-section > li.devsite-nav-item"
    )

    # TODO: can fix not using index??
    NeededRawData = ResourceRawData[1:3]
    NeededRawData.append(ResourceRawData[63])

    ExtractedRawData = []
    for rawData in NeededRawData:
        ExtractedRawData.extend(rawData.select("li.devsite-nav-item > a"))

    resources = []
    prefixHref = "https://developers.google.com/"

    for idData in range(len(ExtractedRawData)):
        category = "resources"
        if idData == 0:  # case segments
            category = "segments"
        elif idData == 1:  # case metrics
            category = "metrics"
        resources.append(
            {
                "Category": category,
                "Link": prefixHref + ExtractedRawData[idData]["href"],
                "Name": ExtractedRawData[idData].text,
            }
        )
    return resources

# 
def downloadXmlRawData(resourceLink):
    folderName = datetime.now().strftime("%Y%m%d") + "_googleadsData/"
    folderPath = "rawdata/" + folderName
    checkRawdataFolder(folderPath)

    for id, element in enumerate(resourceLink):  #
        downloadXml(element["Link"], folderPath + element["Category"], element["Name"])
        print(element)
        if id > 10:
            break

    return folderName


def downloadXml(link, folderName, fileName):
    # Send a GET request to the URL
    response = requests.get(link)

    # Create BeautifulSoup object from the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # Remove script tags
    for script in soup.find_all("script"):
        script.extract()

    # Create the root element for the XML document
    root_element = soup.html

    # Create the XML string
    xml_string = root_element.prettify()

    xml_file_path = folderName + "/" + fileName + ".xml"

    # Write the XML string to a file
    with open(xml_file_path, "w", encoding="utf-8") as xml_file:
        xml_file.write(xml_string)


def checkRawdataFolder(folder_name):
    if not os.path.isdir(folder_name):  # if folder not exist, will create new folder
        os.makedirs(folder_name + "/segments")
        os.makedirs(folder_name + "/metrics")
        os.makedirs(folder_name + "/resources")


def copyToInput(folderName):
    # checkInputFolder(folderName)
    shutil.copytree(
        "./rawdata/" + folderName, "./inputdata/" + folderName
    )  # cai nay folder dest ko dc ton tai trc khi chay


def checkInputFolder(folderName):  # neu folder ton tai, se ko copy dc.
    # if not os.path.isdir("inputdata/"):
    os.makedirs("inputdata/" + folderName)


def writeLog(folderName):
    file_path = "config/foldername.properties"
    with open(file_path, 'w') as file:
        file.write("folder_name={}".format(folderName[:-1]))


if __name__ == "__main__":
    googleadsLink = "https://developers.google.com/google-ads/api/fields/v15/overview"
    googleadsRawData = getHtmlData(googleadsLink)

    resourceLink = extractInfoResource(googleadsRawData)
    # print(len(resourceLink))
    folderName = downloadXmlRawData(resourceLink)
    writeLog(folderName)