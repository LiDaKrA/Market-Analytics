# ---------------------------------------------------------------------
# Description
# ---------------------------------------------------------------------
# This script will be a prototype of a class of scripts that will be used to translate information 
# from the Darknet-Market-Dataset published on https://www.gwern.net/DNM%20archives to a postgressql database.
# After the data is fed into the database, various analysis can take place based on the then structured data.
# A visualization of the database schema is available at ??
# ---------------------------------------------------------------------
# Issues to be worked on
# ---------------------------------------------------------------------
# Database Schema
# ReviewIds and procedure
# Changes in Public key and profile (via Hashes?)
# Identify products with different names (via Review profile)
# Database Size? Keep an eye on the space requirements as the database grows
# Relationship to pictures?
# Documentation
# Documentation of advanced queries in postgressql
# ---------------------------------------------------------------------
# Recent Changes
# ---------------------------------------------------------------------
# Fixed duplicate Handler-Creation in the logging module
# Implemented Category Class


# ------Make Changes here (if neccesary)
# Database Connection
db_string = 'postgres://postgres:asdf@localhost/ProductTest'   
# Data source down to the folder containing folders of format "2015-05-01"
data_source='C://coderun'   
# Path containing secondary sources like exchangerates between USD and BTC
param_dir='C://USB/BITSTAMP-USD.csv'
output_dir = 'C://USB/'

import pandas as pd
from bs4 import BeautifulSoup
import time 
import dateutil 
import os
import logging
from datetime import timedelta
import re
from model import *


marketplace = session.query(Marketplaces).filter(Marketplaces.marketplace_id=="SR2").all()
if not marketplace:
    newmarketplace = Marketplaces(marketplace_id='SR2', marketplace_description="Silkroad2")  
    session.add(newmarketplace)  
    try: session.commit() 
    except: session.rollback()
    session.close()

FORMAT = '%(asctime)-15s %(levelname)-6s %(message)s'
DATE_FORMAT = '%b %d %H:%M:%S'
formatter = logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
while logger.handlers:
     logger.handlers.pop()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def CollectVendorInformation(userdir, fdate):
    logger.info('processing: {}'.format(userdir))
    with open(userdir, encoding='utf8') as userdir:
        soup = BeautifulSoup(userdir, "lxml")
    try:
        vendor = soup.find_all("div", attrs={'class', 'h1'})[0].get_text().strip()
    except:
        return
    if vendor != 'gwern':
        raw = soup.find_all("span", attrs={'class', 'container'})[0].get_text().strip()
    
        vendorSince = raw[raw.index("has been a vendor for")+21: raw.index("was last seen")]
        if 'day' in vendorSince:
            vendorSince = vendorSince.replace("days","").replace("day","").replace("about","").strip()
        elif 'month' in vendorSince:
            vendorSince = vendorSince.replace("months","").replace("month","").replace("about","").strip()
        vendorSince = int(vendorSince)*30
        
        vendorSince = fdate - timedelta(days=vendorSince)
        if len(str(vendorSince.month)) ==  1: month = '0' + str(vendorSince.month) 
        else: month = str(vendorSince.month)
        if len(str(vendorSince.day)) ==  1: day = '0'+str(vendorSince.day) 
        else: day = str(vendorSince.day)
        vendorSince = str(vendorSince.year) + month + day
        vendorSince = int(vendorSince)
    
        profile = soup.find_all("div", attrs={'class', 'container container_large'})[0].get_text().strip()
        try:
            pgp_key = soup.find_all("textarea", attrs={'class', 'container container_large'})[0].get_text().strip()
        except:
            pgp_key = None
        vendors = session.query(Vendor).filter(Vendor.marketplace_id=="SR2").filter(Vendor.vendor_name==vendor).all()
        if not vendors:
           newVendor = Vendor(marketplace_id="SR2", vendor_name=vendor, vendorSince=vendorSince, profile=profile, pgp_key=pgp_key)  
           session.add(newVendor)  
           try: session.commit() 
           except: session.rollback() 
        else:
           vendors[0].vendorSince = vendorSince 
           vendors[0].profile = profile 
           vendors[0].pgp_key = pgp_key 
           try: session.commit() 
           except: session.rollback() 
        session.close()
        return

def CollectCategories(category, fname, fdate, catdir):
    logger.info('processing: {}'.format(fname))
    with open(fname, encoding='utf8') as fname:
        soup = BeautifulSoup(fname, "lxml")
    if not soup.find(string=re.compile("no listings found")):
        collect = soup.find_all("div", attrs={'class', 'item'})
         
        category = category.split('-')
        category_1= category[0]
        try: category_2= category[1]
        except: category_2 = None
        try: category_3= category[2]
        except: category_3 = None
        try: category_4= category[3]
        except: category_4 = None
        try: category_5= category[4]
        except: category_5 = None
        try: category_6= category[5]
        except: category_6 = None

        categories = session.query(Categories).filter(Categories.marketplace_id=="SR2").filter(Categories.category_1==category_1).filter(Categories.category_2==category_2).filter(Categories.category_3==category_3).filter(Categories.category_4==category_4).filter(Categories.category_5==category_5).filter(Categories.category_6==category_6).all()

        if not categories:
            newCategory = Categories(marketplace_id="SR2", category_1=category_1, category_2=category_2, category_3=category_3, category_4=category_4, category_5=category_5, category_6=category_6)  
            session.add(newCategory)  
            session.commit() 
            categories = session.query(Categories).filter(Categories.marketplace_id=="SR2").filter(Categories.category_1==category_1).filter(Categories.category_2==category_2).filter(Categories.category_3==category_3).filter(Categories.category_4==category_4).filter(Categories.category_5==category_5).filter(Categories.category_6==category_6).all()
        category_id = categories[0].category_id
           
           
        for p in collect:
            try:
                name = p.find_next("div", attrs={'class', 'item_title'}).get_text().strip()
                vendor = p.find_next("div", attrs={'class', 'item_details'}).find_next("a").get_text().strip()
                products = session.query(Products).filter(Products.name==name).filter(Products.vendor==vendor).all()
                for match in products:
                    match.category_id = category_id  
                    try: session.commit() 
                    except: session.rollback() 
            except:
                pass
        session.close()
        return
    else:
        return


def CollectProducts(pagelist, fdate, catdir, exchange_rate):
    for fname in pagelist:
        if fname == pagelist[0]:
            try:
                logger.info('processing: {}'.format(fname))
                with open(fname, encoding='utf8') as fname:
                    soup = BeautifulSoup(fname, "lxml")
                    if soup.find_all(id = 'content') and soup.find_all('h2'):  
                        
                        
                        # Vendor Class
                        vendorcand = soup.find_all('h3')
                        for h3 in vendorcand:
                            candidate = h3.get_text()
                            if 'vendor' in candidate:
                                vendor = list(h3.children)[1].get_text().strip()
                                # breaking from this is crucial
                                break      
                        vendors = session.query(Vendor).filter(Vendor.marketplace_id=="SR2").filter(Vendor.vendor_name==vendor).all()
                        if not vendors:
                            newVendor = Vendor(marketplace_id="SR2", vendor_name=vendor, pgp_key="", profile="", vendorSince=None)  
                            session.add(newVendor)  
                            session.commit() 
                            vendors = session.query(Vendor).filter(Vendor.marketplace_id=="SR2").filter(Vendor.vendor_name==vendor).all()
    
                        vendor_id = vendors[0].vendor_id
    
                        
                        # Product Class                        
                        product = soup.find_all('h2')[0].get_text().strip()                        
                        shippinginfo = soup.find_all('h3')[0].find_next('p').get_text()           
                        if 'ships from' and 'ships to' in shippinginfo:
                            origin = shippinginfo[shippinginfo.index('ships from')+12:shippinginfo.index('ships to')].strip()
                            destination = shippinginfo[shippinginfo.index('ships to')+10:].strip()
                        elif 'ships from' in shippinginfo:
                            origin = shippinginfo[shippinginfo.index('ships from')+12:].strip()
                            destination = ""                
                        elif 'ships to' in shippinginfo:
                            origin = ""
                            destination = shippinginfo[shippinginfo.index('ships to')+10:].strip()      
                        else:
                            origin = ""
                            destination = ""  
    
                        if fdate.day > 9: 
                            crawlDay = fdate.day
                        else: crawlDay = "0" + str(fdate.day)
                        if fdate.month > 9: 
                            fdate.month
                        else: crawlMonth = crawlMonth = "0" + str(fdate.month)
                        currentTimestamp = str(fdate.year) + str(crawlMonth) + str(crawlDay)
                        currentTimestamp = int(currentTimestamp)
                        
                        products = session.query(Products).filter(Products.marketplace_id=="SR2").filter(Products.name==product).filter(Products.vendor==vendor).all()
                        if not products:
                            newproduct = Products(marketplace_id="SR2", name=product, vendor_id = vendor_id, vendor=vendor, category = "", origin=origin, destination=destination, last_seen = currentTimestamp, first_seen = currentTimestamp, date=currentTimestamp)  
                            session.add(newproduct)  
                            try: session.commit() 
                            except: session.rollback()    
                        else:
                            if products[0].last_seen > currentTimestamp:
                                pass
                            else:
                                products[0].last_seen = currentTimestamp  
                                try: session.commit() 
                                except: session.rollback() 
                                   
                            products = session.query(Products).filter(Products.vendor_id==vendor_id).filter(Products.name==product).all()
                            if products[0].first_seen < currentTimestamp:
                                pass
                            else:
                                products[0].first_seen = currentTimestamp  
                                   
    
                        products = session.query(Products).filter(Products.vendor_id==vendor_id).filter(Products.name==product).all()
                        product_id = products[0].product_id
                        
    
                        
                        #------ Price Class
                        price = soup.find_all('div', attrs={'class', 'price_big'})[0].get_text().replace("฿","").strip()
                        price = float(price)
                        price_dollar = price * exchange_rate
                        
                        prices = session.query(Prices).filter(Prices.product_id==product_id).filter(vendor_id==vendor_id).all()
                        if not prices:
                            newPrice = Prices(marketplace_id="SR2", product_id=product_id, timestamp=currentTimestamp, price=price, exchange_rate=exchange_rate, price_dollar=price_dollar)  
                            session.add(newPrice)  
                            try: session.commit() 
                            except: session.rollback()  
                        else:
                            isThere = False
                            for i in range(0, len(prices)):
                                if currentTimestamp == prices[i].timestamp and price == prices[i].price:
                                    isThere = True
                                    break
                            if isThere != True:
                                newPrice = Prices(marketplace_id="SR2", product_id=product_id, timestamp=currentTimestamp, price=price, exchange_rate=exchange_rate, price_dollar=price_dollar)  
                                session.add(newPrice)
                                try: session.commit() 
                                except: session.rollback() 
                            
                        
                        #------ Review Class
                        itemfeedbcand = soup.find_all('h3')
                        reviews = 0
                        for cand in itemfeedbcand:
                            if 'item feedback' in cand:
                                thistable = cand.find_next('table')
                                reviews = len(thistable.find_all('tr'))-1
                                break
    
                        for i in range(1, int(reviews)+1): 
                            evaluation = thistable.find_all('tr')[i].find_all('td')[0].get_text().replace(' of 5','')
                            if '★' in evaluation:
                                evaluation = "none"
                            reviewtext = thistable.find_all('tr')[i].find_all('td')[1].get_text()
                            timestamp = thistable.find_all('tr')[i].find_all('td')[2].get_text()
                            if len(timestamp.split(' ')) >2:
                                raise NameError
                            else: 
                                if timestamp == 'today':
                                    days = 0;
                                else:
                                    days = int(timestamp.split(' ')[0])
                            
                            timestamp = fdate - timedelta(days=days)
                            if len(str(timestamp.month)) ==  1: month = '0' + str(timestamp.month) 
                            else: month = str(timestamp.month)
                            if len(str(timestamp.day)) ==  1: day = '0'+str(timestamp.day) 
                            else: day = str(timestamp.day)
                            timestamp = str(timestamp.year) + month + day
                            timestamp = int(timestamp)
    
                            prices = session.query(Prices).filter(Prices.product_id==product_id).filter(vendor_id==vendor_id).filter(timestamp<=timestamp).order_by('timestamp desc')
                            if prices:
                                reviewPrice = prices[0].price
                            else:
                                reviewPrice = session.query(Prices).filter(Prices.product_id==product_id).filter(vendor_id==vendor_id).filter(timestamp<=timestamp).order_by('timestamp desc')[0].price
     
                            newReview = Reviews(marketplace_id="SR2", product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext)  
                            session.add(newReview)  
                            try: session.commit() 
                            except: session.rollback() 
                        
                        if len(soup.find_all('span', attrs={'class','next'})) < 2:
                            session.close()
                            return
                        
                        session.close()
                        return

            except FileNotFoundError:
                # try output_dir as global variable
                e = open(os.path.join(output_dir,'error2.csv'),'a')
                try:
                    e.write('SSE' + fname.name + '\n')
                except:
                    e.write('SSE' + fname + '\n')
                e.close()
    
def Directory_Handler_Cat(category, catdir, fdate):
    fsdir = os.listdir(catdir)
    fsdir = map(lambda x: os.path.join(catdir,x),fsdir)
    for f in fsdir:
        CollectCategories(category, f,fdate,catdir)
    return

def Directory_Handler_Users(userdir, fdate):
    if not ('_feedback_page=' in userdir or ".2" in userdir or ".3" in userdir or ".4" in userdir or ".5" in userdir or ".6" in userdir or ".7" in userdir or ".8" in userdir or ".9" in userdir):
        CollectVendorInformation(userdir, fdate)
    return
    
def Directory_Handler_Products(catdir, fdate, exchange_rate):
    fsdir = sorted(os.listdir(catdir))
    for i in range(0, len(fsdir)):
        # Collect pages that belong together
        pages =[]
        if not '_feedback_page=' in fsdir[i]:
            pages.append(os.path.join(catdir,fsdir[i])) 
            for page in fsdir[i+1:]:
                if '_feedback_page=' in page:
                    pages.append(os.path.join(catdir,page))
                else:
                    break

            CollectProducts(pages, fdate, catdir, exchange_rate)
        else:
            pass

    return
            
def main():
    source = pd.read_csv(param_dir,index_col='Date', sep=',', engine='python')
    crawls = sorted(os.listdir(data_source))
    for datestr in crawls: #['2014-04-12']
        try:
            exchange_rate = source.loc[datestr,'VWAP']               
        except KeyError:
            exchange_rate = source.loc['2014-04-15','VWAP']
        tpath = os.path.join(data_source, datestr)
        if os.path.isdir(tpath):
            if os.path.exists(os.path.join(tpath, "items")):
                xpath = os.path.join(tpath, "items")
                fdate = dateutil.parser.parse(datestr)
                logger.info(xpath)
                Directory_Handler_Products(xpath, fdate, exchange_rate)    
            if os.path.exists(os.path.join(tpath, "categories")):
                xpath = os.path.join(tpath, "categories")
                for cat in os.listdir(xpath):
                    catdir = os.path.join(xpath,cat)
                    if os.path.isdir(catdir):
                        fdate = dateutil.parser.parse(datestr)
                        logger.info(catdir)
                        Directory_Handler_Cat(cat, catdir, fdate)
            if os.path.exists(os.path.join(tpath, "users")):
                xpath = os.path.join(tpath, "users")
                for userpage in os.listdir(xpath):
                    userdir = os.path.join(xpath,userpage)
                    if not os.path.isdir(userdir):
                        fdate = dateutil.parser.parse(datestr)
                        logger.info(userdir)
                        Directory_Handler_Users(userdir, fdate)

if __name__=='__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))