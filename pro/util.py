import os
from Silkroad2_ne import data_source
from bs4 import BeautifulSoup
from datetime import timedelta
    
def PullLastCrawlDate(fdate):
    crawls = sorted(os.listdir(data_source))
    for i in range(len(crawls)):
        if fdate == crawls[0]:
            return '1990-01-01'
        elif fdate == crawls[i]:
            return crawls[i-1]


def Order_key(pagename):
    if pagename.count("_feedback_page=")==0:
        return int(0)
    elif pagename.count("_feedback_page=")==1:
        return int(pagename[pagename.index("_feedback_page=")+15:])
    elif pagename.count("_feedback_page=")==2:
        return int(pagename[pagename.index("_feedback_page=")+15:pagename.index("&")])          


def TransformTimestamp(fdate):
    if fdate.day > 9: 
        crawlDay = fdate.day
    else: crawlDay = "0" + str(fdate.day)
    if fdate.month < 9: 
        crawlMonth = "0" + str(fdate.month)
    else:
        crawlMonth = fdate.month
    currentTimestamp = str(fdate.year) + str(crawlMonth) + str(crawlDay)
    currentTimestamp = int(currentTimestamp)
    return currentTimestamp