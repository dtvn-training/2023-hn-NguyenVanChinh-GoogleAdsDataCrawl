import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import logging
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
def extractLink(googleadsRawData):
    soupGoogleadsData = BeautifulSoup(googleadsRawData, "html.parser")
    # extract item from navigation bar
    ReportsLink = soupGoogleadsData.select(
        "section devsite-header div div.devsite-collapsible-section devsite-tabs nav.devsite-tabs-wrapper tab:nth-child(4) a"
    )
    return ReportsLink[0]["href"]


# write result of crawlCode to log file
def writeToLogFile(folderName, resourceLink):
    # Configure logging to write to a file
    logging.basicConfig(
        # filename="log/crawls.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info(
        "Downloaded file to folder: inputdata/{}, with {} file".format(
            folderName, len(resourceLink)
        )
    )


# open file properties get link to googleads website
def getLinkGoogleads(getNewest=False):
    if getNewest:
        file_path = "config/newestLink.properties"
    else:
        file_path = "config/googleadsLink.properties"
    properties = {}
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=")
                properties[key.strip()] = value.strip().strip("'")
    return properties.get("googleadsLink")


def writeToNewestLink(googleadsLink):
    file_path = "config/newestLink.properties"
    with open(file_path, "w") as file:
        file.write("googleadsLink={}".format(googleadsLink))


# run process using above function
def updateNewestLinkGoogleads():
    homeGoogleadsLink = "https://developers.google.com/google-ads/api/docs/start"

    googleadsRawData = getHtmlData(homeGoogleadsLink)
    googleadsReportLink = extractLink(googleadsRawData)
    writeToNewestLink(googleadsReportLink)
