# ---------------------------------------------------------------------
# Warning: This part is under construction and may raise errors
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
# Do references stay constant over time?
# Convert pictures from base64 to jpeg
# logging levels
# Database Queries: document some joins && make script leaner
# ---------------------------------------------------------------------



try:
    session.rollback()
except:
    pass
# ------Make Changes here (if neccesary)
# Database Connection
db_string = "postgres://postgres:asdf@localhost/ProductTest"  
# Data source down to the folder containing folders of format "2015-05-01"
# data_source="C://coderun"
data_source="D://dnmarchives/Silkroad2/silkroad2"
# Path containing secondary sources like exchangerates between USD and BTC
param_dir="C://USB/BITSTAMP-USD.csv"
output_dir = "C://USB/"

import pandas as pd
from bs4 import BeautifulSoup
import time 
import dateutil 
import os
import logging
from datetime import timedelta
import re
from model import *
from util import *

marketplace = session.query(Marketplaces).filter(Marketplaces.marketplace_id=='SR2').all()
if not marketplace:
    newmarketplace = Marketplaces(marketplace_id='SR2', marketplace_description="Silkroad2")  
    session.add(newmarketplace)  
    session.commit() 

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

FORMAT = "%(asctime)-15s %(levelname)-6s %(message)s"
DATE_FORMAT = "%b %d %H:%M:%S"
formatter = logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT)

file_handler = logging.FileHandler("errors.log")
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

while logger.handlers:
     logger.handlers.pop()

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def CleanVendorExpression(userdir):   
    lastVendor = userdir[userdir.index("users\\")+6:]
    if lastVendor.count("_vendor_feedback_page")==1:
        lastVendor = lastVendor[:lastVendor.index('_vendor_feedback_page')]
    elif lastVendor.count("_feedback_page")==1:
        lastVendor = lastVendor[:lastVendor.index('_feedback_page')]
    lastVendor = lastVendor.replace(".2","").replace(".3","").replace(".3","").replace(".4","").replace(".5","").replace(".6","").replace(".7","").replace(".8","").replace(".9","")
    return lastVendor 


def CheckForNovelty(pagelist, fdate, vendor_id):
    # perform fast check via PullLastCrawlDate        
    with open(pagelist[len(pagelist)-1], encoding='utf8') as fname:
        soup = BeautifulSoup(fname, "lxml")
        if not soup.find_all(id = 'content') or not soup.find_all('h2'):
            with open(pagelist[len(pagelist)-2], encoding='utf8') as fname:
                soup = BeautifulSoup(fname, "lxml")

        itemfeedbcand = soup.find_all('h3')
        reviews = 0
        for cand in itemfeedbcand:
            if "item feedback" in cand:
                thistable = cand.find_next('table')
                reviews = len(thistable.find_all('tr'))-1
                break

        if reviews > 0:
            timestamp = thistable.find_all('tr')[int(reviews)-1+1].find_all('td')[2].get_text()
            if len(timestamp.split(' ')) >2:
                raise NameError
            else: 
                if timestamp == 'today':
                    days = 0;
                else:
                    days = int(timestamp.split(' ')[0])
        
            timestamp = fdate - timedelta(days=days)
            timestamp = TransformTimestamp(timestamp)
            
            mdate = str(TransformTimestamp(fdate))                     
            mdate = mdate[0:4] + '-' + mdate[4:6] + '-' + mdate[6:8]
            lastCrawl = PullLastCrawlDate(mdate)
            lastCrawl = int(lastCrawl.replace("-","").replace("-",""))
            if timestamp > lastCrawl:
                return True, None
            else:
                # Further tests have to be done 
                # Query all available Reviews on this site
                collection = []
                for i in range(1, reviews): 
                    evaluation = thistable.find_all('tr')[int(reviews)+1-i].find_all('td')[0].get_text().replace(" of 5","")
                    if "★" in evaluation:
                        evaluation = None
                    reviewtext = thistable.find_all('tr')[int(reviews)+1-i].find_all('td')[1].get_text()
                    timestamp = thistable.find_all('tr')[int(reviews)+1-i].find_all('td')[2].get_text()
                    if len(timestamp.split(' ')) >2:
                        raise NameError
                    else: 
                        if timestamp == 'today':
                            days = 0;
                        else:
                            days = int(timestamp.split(' ')[0])
                
                    timestamp = fdate - timedelta(days=days)
                    timestamp = TransformTimestamp(timestamp)
                    
                    # Query the database based on evaluation date (plus minus1), reviewtext, vendor and marketplace
                    entry = []
                    reviews0 = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.vendor_id==vendor_id, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext, Reviews.timestamp==timestamp-2)).all() 
                    reviews1 = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.vendor_id==vendor_id, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext, Reviews.timestamp==timestamp-1)).all() 
                    reviews2 = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.vendor_id==vendor_id, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext, Reviews.timestamp==timestamp)).all()  
                    reviews3 = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.vendor_id==vendor_id, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext, Reviews.timestamp==timestamp+1)).all() 
                    reviews4 = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.vendor_id==vendor_id, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext, Reviews.timestamp==timestamp+2)).all() 
                    for review in reviews1:
                        entry.append(review.product_id)
                    for review in reviews2:
                        entry.append(review.product_id)
                    for review in reviews3:
                        entry.append(review.product_id)
                    for review in reviews0:
                        entry.append(review.product_id)
                    for review in reviews4:
                        entry.append(review.product_id)
                    collection.append(entry)
                logger.info(pagelist[0])
                logger.info(collection)
                
                # Once thats done for all reviews present on the site
                # Find common elements (product_ids)
                if len(collection)>1:
                    candidate = set(collection[0]).intersection(*collection)
                    logger.info(candidate)
                    if candidate:
                        return False, candidate
                    else:
                        return True, None
                else: 
                    return True, None
        else:
            return True, None 
        

def CollectVendorInformation(userdir, fdate):
    print(userdir)
    input("Press")
    #logger.info('processing: {}'.format(userdir))
    with open(userdir, encoding='utf8') as userdir:
        soup = BeautifulSoup(userdir, 'lxml')
    try:
        vendor = soup.find_all('div', attrs={'class', 'h1'})[0].get_text().strip()
    except:
        return
    if vendor != 'gwern':

        raw = soup.find_all('span', attrs={'class', 'container'})[0].get_text().strip()
    
        vendorSince = raw[raw.index("has been a vendor for")+21: raw.index("was last seen")]
        if 'day' in vendorSince:
            vendorSince = vendorSince.replace("days","").replace("day","").replace("about","").strip()
        elif 'month' in vendorSince:
            vendorSince = vendorSince.replace("months","").replace("month","").replace("about","").strip()
        vendorSince = int(vendorSince)*30
        
        vendorSince = fdate - timedelta(days=vendorSince)
        vendorSince = TransformTimestamp(vendorSince)

        vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=="SR2", Vendors.vendor_name==vendor)).first()
        if not vendors:
           newVendor = Vendors(marketplace_id='SR2', vendor_name=vendor, vendorSince=vendorSince)  
           session.add(newVendor)  
           session.commit() 

        else:
           vendors.vendorSince = vendorSince  
           session.commit() 

        try:
            pgp_key = soup.find_all('textarea', attrs={'class', 'container container_large'})[0].get_text().strip()
        except:
            pgp_key = None
            
        fdate = TransformTimestamp(fdate)
        
        vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='SR2', Vendors.vendor_name==vendor)).first()
        vendor_id = vendors.vendor_id
        
        keys = session.query(PGP_Keys).filter(and_(PGP_Keys.marketplace_id=='SR2', PGP_Keys.vendor_id==vendor_id, PGP_Keys.pgp_key == pgp_key)).first()  
        if keys == None:
            newpgp = PGP_Keys(marketplace_id='SR2', vendor_id=vendor_id, timestamp=fdate, pgp_key=pgp_key)  
            session.add(newpgp)  
            session.commit() 
                    
        thisProfile = soup.find_all('div', attrs={'class', 'container container_large'})[0].get_text().strip()
        profiles = session.query(Vendorprofiles).filter(and_(Vendorprofiles.marketplace_id=='SR2', Vendorprofiles.vendor_id==vendor_id, Vendorprofiles.profile==thisProfile)).first()  
        if profiles == None:
            newProfile = Vendorprofiles(marketplace_id='SR2', vendor_id=vendor_id, timestamp=fdate, profile=thisProfile)  
            session.add(newProfile)  
            session.commit() 
        
        return



def CollectCategories(category, fname, fdate):
    logger.info('processing: {}'.format(fname))
    with open(fname, encoding='utf8') as fname:
        soup = BeautifulSoup(fname, 'lxml')
    if not soup.find(string=re.compile("no listings found")):
        collect = soup.find_all('div', attrs={'class', 'item'})
         
        category = category.split('-')
        category_1= category[0].capitalize()
        try: category_2= category[1].capitalize()
        except: category_2 = None
        try: category_3= category[2].capitalize()
        except: category_3 = None
        try: category_4= category[3].capitalize()
        except: category_4 = None
        try: category_5= category[4].capitalize()
        except: category_5 = None
        try: category_6= category[5].capitalize()
        except: category_6 = None
        cat = []
        cat.append(category_1)
        cat.append(category_2)
        cat.append(category_3)
        cat.append(category_4)
        cat.append(category_5)
        cat.append(category_6)
        compressedCategory = ""
        for i in range(0,6):
            if i == 0:
                compressedCategory += cat[i]
            elif cat[i] != None:
                compressedCategory += "|"
                compressedCategory += cat[i]

        categories = session.query(Categories).filter(and_(Categories.marketplace_id=='SR2', Categories.category_1==category_1, Categories.category_2==category_2, Categories.category_3==category_3, Categories.category_4==category_4, Categories.category_5==category_5, Categories.category_6==category_6)).first()

        if not categories:
            newCategory = Categories(marketplace_id='SR2', category_1=category_1, category_2=category_2, category_3=category_3, category_4=category_4, category_5=category_5, category_6=category_6)  
            session.add(newCategory)  
            session.commit() 
            categories = session.query(Categories).filter(and_(Categories.marketplace_id=='SR2', Categories.category_1==category_1, Categories.category_2==category_2, Categories.category_3==category_3, Categories.category_4==category_4, Categories.category_5==category_5, Categories.category_6==category_6)).first()
        category_id = categories.category_id
           
        for p in collect:
            try:
                name = p.find_next('div', attrs={'class', 'item_title'}).get_text().strip()
                vendor = p.find_next('div', attrs={'class', 'item_details'}).find_next('a').get_text().strip()
                products = session.query(Products).filter(and_(Products.marketplace_id=='SR2', Products.name==name, Products.vendor==vendor)).all()
                for match in products:
                    match.category_id = category_id 
                    match.category = compressedCategory
                    session.commit() 

            except:
                pass
        return
    else:
        return



def CollectProducts(pagelist, fdate, exchange_rate):
    try:
        logger.info('processing: {}'.format(pagelist[0]))
        with open(pagelist[0], encoding='utf8') as fname:
            soup = BeautifulSoup(fname, 'lxml')
            if soup.find_all(id = 'content') and soup.find_all('h2'):  
    
                # Vendor Class
                #--------- Find the vendors name on the page
                vendorcand = soup.find_all('h3')
                for h3 in vendorcand:
                    candidate = h3.get_text()
                    if 'vendor' in candidate:
                        vendor = list(h3.children)[1].get_text().strip()
                        # breaking from this is crucial
                        break      
                #--------- Find out if the vendor is already in the database
                #--------- if not make an entry for the new vendor
                vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='SR2', Vendors.vendor_name==vendor)).first()
                if not vendors:
                    newVendor = Vendors(marketplace_id='SR2', vendor_name=vendor, vendorSince=None)  
                    session.add(newVendor)  
                    session.commit() 
                    vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='SR2', Vendors.vendor_name==vendor)).first()
                #--------- Find the vendor_id for later reference
                vendor_id = vendors.vendor_id

                
                # Product Class    
                #--------- Find the product title and shipping information
                product = soup.find_all('h2')[0].get_text().strip()                        
                shippinginfo = soup.find_all('h3')[0].find_next('p').get_text()           
                if "ships from" and "ships to" in shippinginfo:
                    origin = shippinginfo[shippinginfo.index("ships from")+12:shippinginfo.index("ships to")].strip()
                    destination = shippinginfo[shippinginfo.index("ships to")+10:].strip()
                elif "ships from" in shippinginfo:
                    origin = shippinginfo[shippinginfo.index("ships from")+12:].strip()
                    destination = None                
                elif "ships to" in shippinginfo:
                    origin = None
                    destination = shippinginfo[shippinginfo.index("ships to")+10:].strip()      
                else:
                    origin = None
                    destination = None 

                currentTimestamp = TransformTimestamp(fdate)
                                        
                #--------- Look if product (with this title) belonging to the same marketplace and vendor exists  
                products = session.query(Products).filter(and_(Products.marketplace_id=='SR2', Products.name==product, Products.vendor_id==vendor_id)).first()
                if not products:
                    #--------- Product from this vendor is not in the database, but is it really new?
                    status, candidates = CheckForNovelty(pagelist, fdate, vendor_id)
                    if status:                       
                        #--------- if it is new add new product with static information
                        newproduct = Products(marketplace_id='SR2', name=product, vendor_id = vendor_id, vendor=vendor, category_id=None, category=None, origin=origin, destination=destination, last_seen = currentTimestamp, first_seen = currentTimestamp, date=currentTimestamp)  
                        session.add(newproduct)  
                        session.commit()   
                        products = session.query(Products).filter(and_(Products.marketplace_id=='SR2', Products.vendor_id==vendor_id, Products.name==product)).first()
                        product_id = products.product_id
                        
                    else:
                        #--------- take original product_id from CheckForNovelty and update values on existsting product_id
                        product_id = candidates.pop()
                        products = session.query(Products).filter(and_(Products.marketplace_id=='SR2', Products.vendor_id==vendor_id, Products.product_id==product_id)).first()
                        products.destination = destination
                        if products.last_seen < currentTimestamp:
                            products.last_seen = currentTimestamp 
                        products.origin = origin
                        products.name = product
                        session.commit()                               
                else:
                    # Product belonging to that vendor and the same title was found
                    # Vendor & Title are the same, update origin, destination, last_seen, date
                    product_id = products.product_id
                    if products.last_seen < currentTimestamp:
                        products.last_seen = currentTimestamp 
                    products.origin = origin
                    products.destination = destination
                    session.commit()       
                           
                #-------- product_id was established as a reference to this product

                 #------ Price Class
                price = soup.find_all('div', attrs={'class', 'price_big'})[0].get_text().replace("฿","").strip()
                price = float(price)
                price_dollar = price * exchange_rate

                prices = session.query(Prices).filter(and_(Prices.marketplace_id=='SR2', Prices.product_id==product_id, Prices.vendor_id==vendor_id)).all()
                if prices == None:
                    newPrice = Prices(marketplace_id='SR2', product_id=product_id, timestamp=currentTimestamp, price=price, vendor_id=vendor_id, exchange_rate=exchange_rate, price_dollar=price_dollar)  
                    session.add(newPrice)  
                    session.commit()  
                else:
                    isThere = False
                    for i in range(0, len(prices)):
                        if currentTimestamp == prices[i].timestamp and price == prices[i].price:
                            isThere = True
                            break
                    if isThere != True:
                        newPrice = Prices(marketplace_id='SR2', product_id=product_id, timestamp=currentTimestamp, price=price, exchange_rate=exchange_rate, price_dollar=price_dollar, vendor_id=vendor_id)  
                        session.add(newPrice)
                        session.commit() 
                                
                #------ Productprofile Class        
                thisProdProfile = soup.find_all('div', attrs={'class', 'container container_large'})[0].get_text().strip()
                profiles = session.query(Productprofiles).filter(and_(Productprofiles.marketplace_id=='SR2', Productprofiles.vendor_id==vendor_id, Productprofiles.profile==thisProdProfile)).first()  
                if profiles == None:
                    newProfile = Productprofiles(marketplace_id='SR2', vendor_id=vendor_id, product_id=product_id, timestamp=currentTimestamp, profile=thisProdProfile)                                                          
                    session.add(newProfile)  
                    session.commit() 
    
                #------ Images Class        
                try:
                    thisImage = soup.find_all('div', attrs={'class', 'item_image main'})[0].get('id')
                except:
                    thisImage = None
                # profiles = session.query(Productprofiles).filter(and_(Productprofiles.marketplace_id=='SR2', Productprofiles.vendor_id==vendor_id, Productprofiles.profile==thisProdProfile)).first()  
                # if profiles == None:

                newImage = Images(marketplace_id='SR2', vendor_id=vendor_id, product_id=product_id, timestamp=currentTimestamp, hals = thisImage)                                                          
                session.add(newImage) 
                session.commit()
                        
    except FileNotFoundError:
        logger.exception("FileNotFound: {}".format(pagelist[0]))
                
    #------ Review Class

    for page in pagelist:
        with open(page, encoding='utf8') as fname:
            
            soup = BeautifulSoup(fname, 'lxml')
            if soup.find_all(id = 'content') and soup.find_all('h2'):  
                
                #------- Froming tuples of form (evaluation, reviewtext, timestamp)
                itemfeedbcand = soup.find_all('h3')
                reviews = 0
                
                for cand in itemfeedbcand:
                    if "item feedback" in cand:
                        thistable = cand.find_next('table')
                        reviews = len(thistable.find_all('tr'))-1
                        break

                for i in range(1, int(reviews)+1): 
                    evaluation = thistable.find_all('tr')[i].find_all('td')[0].get_text().replace(" of 5","")
                    if "★" in evaluation:
                        evaluation = None
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
                    timestamp = TransformTimestamp(timestamp)
                    
                    prices = session.query(Prices).filter(and_(Prices.marketplace_id=='SR2', Prices.product_id==product_id, Prices.vendor_id==vendor_id, Prices.timestamp<=timestamp)).order_by(desc(Prices.timestamp)).first()
                    if prices!=None:
                        reviewPrice = prices.price
                    else:
                        reviewPrice = session.query(Prices).filter(and_(Prices.marketplace_id=='SR2', Prices.product_id==product_id, Prices.vendor_id==vendor_id)).order_by(desc(Prices.timestamp)).first().price
                    # Query the database based on (marketplace_id, vendor_id, product_id), and (reviewtext, evaluation date (plus minus1), evaluation)

                    reviews1 = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.vendor_id==vendor_id, Reviews.product_id==product_id, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext, Reviews.timestamp==timestamp-1)).all() 
                    reviews2 = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.vendor_id==vendor_id, Reviews.product_id==product_id, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext, Reviews.timestamp==timestamp)).all()  
                    reviews3 = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.vendor_id==vendor_id, Reviews.product_id==product_id, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext, Reviews.timestamp==timestamp+1)).all() 
                    if len(reviews1)==0 and len(reviews2)==0 and len(reviews3)==0:
                        # add review to database
                        newReview = Reviews(marketplace_id='SR2', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext)  
                        session.add(newReview)  
                        session.commit() 
                
    return


def main():
    
    products = True
    categories = False
    vendors = False
    
    source = pd.read_csv(param_dir,index_col='Date', sep=',', engine='python')
    crawls = sorted(os.listdir(data_source))
    for datestr in crawls: #['2014-04-12']
        fdate = dateutil.parser.parse(datestr)
        try:
            exchange_rate = source.loc[datestr,'VWAP']               
        except KeyError:
            exchange_rate = source.loc['2014-04-15','VWAP']
        tpath = os.path.join(data_source, datestr)
        if os.path.isdir(tpath):
            if products:
                if os.path.exists(os.path.join(tpath, "items")):
                    xpath = os.path.join(tpath, "items")
                    fsdir = sorted(os.listdir(xpath))
                    for i in range(0, len(fsdir)):
                        #----- Make collections of pages that belong together
                        pages =[]
                        pagenames =[]

                        if not "_feedback_page=" in fsdir[i]:
                            #----- main page "leads" this collection
                            pages.append(os.path.join(xpath,fsdir[i])) 
                            pagenames.append(fsdir[i])
                            leading_page = fsdir[i].split('//')[-1]

                            for page in fsdir[i+1:]:
                                if "_feedback_page=" in page and leading_page == page[0:page.index('_')]:
                                    #----- excluding vendor feedback pages (both vendor and item feedback can overlap, but this is taken care of)
                                    if page.count("_feedback_page=")==1 and not "_vendor_feedback_page" in page:
                                        pages.append(os.path.join(xpath,page))
                                        pagenames.append(page)
                                    elif page.count("_feedback_page=")==2:
                                        isThere = False
                                        for thisPage in pagenames:
                                            if thisPage == page[0: page.index("&vendor")]:
                                                isThere = True
                                                break
                                        if isThere == False:
                                            pages.append(os.path.join(xpath,page))
                                            pagenames.append(page[0: page.index("&vendor")])
                                else:
                                    break                            
                            #----- Sort the list
                            pages = sorted(pages, key=Order_key) 
                            #----- The list pages containing relevant feedback data is passed to CollecProducts
                            CollectProducts(pages, fdate, exchange_rate)

            if categories:
                if os.path.exists(os.path.join(tpath, "categories")):
                    xpath = os.path.join(tpath, "categories")
                    for cat in os.listdir(xpath):
                        catdir = os.path.join(xpath, cat)
                        if os.path.isdir(catdir):
                            fsdir = os.listdir(catdir)
                            fsdir = map(lambda x: os.path.join(catdir,x),fsdir)
                            for f in fsdir:
                                CollectCategories(cat, f,fdate)
                            
            if vendors:
                if os.path.exists(os.path.join(tpath, "users")):
                    xpath = os.path.join(tpath, "users")
                    for userpage in os.listdir(xpath):
                        userdir = os.path.join(xpath, userpage)
                        if not os.path.isdir(userdir):
                            thisVendor = CleanVendorExpression(userdir)
                            try:
                                if lastVendor != thisVendor:
                                    lastVendor = thisVendor
                                    CollectVendorInformation(userdir, fdate)
                            except:
                                lastVendor = thisVendor
                                CollectVendorInformation(userdir, fdate)                                

if __name__=='__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))