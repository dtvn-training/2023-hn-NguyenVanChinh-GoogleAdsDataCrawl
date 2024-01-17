import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
from python.commonFunction import writeAirflowLog
from python.commonFunction import getLinkGoogleads
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


# send request get html raw data by link
def getHtmlData(googleadsLink):
    googleadsResponse = requests.get(googleadsLink)  # get response to link
    if googleadsResponse.status_code == 200:  # if request ok, return html text
        return getRawHtmlWithSelenium(googleadsLink)
    else:  # if not ok, return status code
        return "error, code: {}".format(googleadsResponse.status_code)


def getRawHtmlWithSelenium(googleadsLink):
    service = Service(executable_path=(os.getcwd() + "/chromedriver"))
    options = Options()
    options.headless = True
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1200")

    driver = webdriver.Chrome(options=options, service=service)
    driver.get(googleadsLink)
    source = driver.page_source
    driver.quit()
    return source


# from htmlRawData, extract necessary info of resources: category, name, link
def extractInfoResource(googleadsRawData):
    soupGoogleadsData = BeautifulSoup(googleadsRawData, "html.parser")

    # extract expanded item from navigation bar
    GoogleadsData = soupGoogleadsData.select(
        "div.devsite-mobile-nav-bottom ul.devsite-nav-section > li.devsite-nav-item.devsite-nav-expandable > div.devsite-expandable-nav.expanded"
    )
    ResourceRawData = GoogleadsData[0].select(
        "ul.devsite-nav-section > li.devsite-nav-item"
    )

    # get specific item, id 1 is segments
    NeededRawData = [ResourceRawData[1]]

    idResource = []
    for ir in range(0, len(ResourceRawData)):
        if len(ResourceRawData[ir].select("li.devsite-nav-item > a")) > 1:
            idResource.append(ir)
    # fist 2 record is list ResourceWithMetric and ResourceWithoutMetric
    for id in idResource[:2]:
        NeededRawData.append(ResourceRawData[id])

    ExtractedRawData = []
    # for each neededRawData, select all a tags and append that list to ExtractedRawData
    for rawData in NeededRawData:
        ExtractedRawData.extend(rawData.select("li.devsite-nav-item > a"))

    resources = []  # list store all extracted resources
    prefixHref = "https://developers.google.com/"  # combine with href attribute in each a tag to a full link, can access with https

    for idData in range(len(ExtractedRawData)):
        category = "resources"  # default case is resource (id > 1), include resourcesWithMetrics and resourcesWithoutMetrics
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


# read extracted resources list and download files
def downloadXmlRawData(resources):
    # create a foldername with date today
    folderName = datetime.now().strftime("%Y%m%d") + "_googleadsData/"
    folderPath = "inputdata/" + folderName

    # check to sure that folder exists with 3 subfolder: segments, metrics, resources
    checkRawdataFolder(folderPath)

    # for each resources, download file xml of its href_link
    for id, resource in enumerate(resources):
        downloadXml(
            resource["Link"], folderPath + resource["Category"], resource["Name"]
        )
        # if id == 10:
        #     break

    return folderName


# function send requests to resource link, beautify with beautifulSoup, remove script tags to be readable in Pentaho
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

    # path to save xml file
    xml_file_path = folderName + "/" + fileName + ".xml"

    # Write the XML string to a file
    with open(xml_file_path, "w", encoding="utf-8") as xml_file:
        xml_file.write(xml_string)


# to be sure that rawdata folder exist and ready for save file
def checkRawdataFolder(folder_name):
    if not os.path.isdir(folder_name):  # if folder not exist, will create new folder
        os.makedirs(folder_name + "/segments")
        os.makedirs(folder_name + "/metrics")
        os.makedirs(folder_name + "/resources")






# write result of crawlCode to log file
def writeToLogFile(folderName, resourceLink):
    logText1 = "Link was changed from {} to {}".format(
        getLinkGoogleads(getNewest=False), getLinkGoogleads(getNewest=True)
    )
    writeAirflowLog(logText1)

    logText2 = "Downloaded file to folder: inputdata/{}, with {} files".format(
        folderName, len(resourceLink)
    )
    writeAirflowLog(logText2)




# run crawl process using above function
def executeCrawl():
    googleadsLink = getLinkGoogleads(getNewest=True)
    googleadsRawData = getHtmlData(googleadsLink)

    resourceLink = extractInfoResource(googleadsRawData)
    
    folderName = downloadXmlRawData(resourceLink)
    writeToLogFile(folderName, resourceLink)
