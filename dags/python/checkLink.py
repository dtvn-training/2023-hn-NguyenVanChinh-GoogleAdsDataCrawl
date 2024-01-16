import requests
from bs4 import BeautifulSoup
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from python.selfLog import writeAirflowLog


# send request get html raw data by link
def getHtmlData(googleadsLink):
    googleadsResponse = requests.get(googleadsLink)  # get response to link
    if (
        googleadsResponse.status_code == 200
    ):  # if request ok, call selenium get html data
        return getRawHtmlWithSelenium(googleadsLink)
    else:  # if not ok, return status code
        return "ERROR get html data, code: {}".format(googleadsResponse.status_code)


# selenium call chromeDriver to send request to website, and get html source
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


def writeToNewestLink(googleadsLink):
    file_path = "config/newestLink.properties"
    with open(file_path, "w") as file:
        file.write("googleadsLink={}".format(googleadsLink))


# run process using above function
def updateNewestLinkGoogleads():
    # send request, if ok then get html data
    homeGoogleadsLink = "https://developers.google.com/google-ads/api/docs/start"
    googleadsRawData = getHtmlData(homeGoogleadsLink)
    # TODO: handle get html error

    # extract link report, it is newest link
    googleadsReportLink = extractLink(googleadsRawData)

    # write log
    writeAirflowLog("Get link: {}".format(googleadsReportLink))
    writeToNewestLink(googleadsReportLink)
