# coding: utf-8

"""
---------------------------------------------------------------------
Description
---------------------------------------------------------------------
This script will be used to translate information from the Silkroad2-Marketplace
from the Darknet-Market-Dataset published on https://www.gwern.net/DNM%20archives to a postgressql database.
After the data is fed into the database, various analysis can take place based on the then structured data.
---------------------------------------------------------------------
"""


server_mode = True

if server_mode:
    # ----- Make Changes here
    # Data source down to the folder containing folders of format "2015-05-01"
    data_source= ""
    # Directory for the logs
    output_dir = ""
else:
    # ----- and / or here
    # Data source down to the folder containing folders of format "2015-05-01"
    data_source=""
    # Directory for the logs
    output_dir = ""


from bs4 import BeautifulSoup
import time 
import dateutil 
import os
import logging
from datetime import timedelta, datetime
import re
from model import *
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

FORMAT = "%(asctime)-15s %(levelname)-6s %(message)s"
DATE_FORMAT = "%b %d %H:%M:%S"
formatter = logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT)

file_handler = logging.FileHandler("errors.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)

while logger.handlers:
     logger.handlers.pop()

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def Order_key(pagename):
    """
    Key for sorting filepaths
    """
    
    if pagename.count("feedback_page=")==0:
        return int(0)
    elif pagename.count("feedback_page=")==1:
        return int(pagename[pagename.index("feedback_page=")+14:])
    else:
        return int(pagename[pagename.index("feedback_page=")+14:pagename.index("&",pagename.index("feedback_page"))])          

def CleanVendorExpression(userdir):   
    """
    Determines the vendorname from vendor-filepath
    """
    
    if "_vendor_feedback_page" in userdir:
        userdir = userdir[:userdir.index('vendor_feedback_page')-1]
    if "_feedback_page" in userdir:
        userdir = userdir[:userdir.index('feedback_page')-1]
    userdir = userdir.replace(".1","").replace(".2","").replace(".3","").replace(".4","").replace(".5","").replace(".6","").replace(".7","").replace(".8","").replace(".9","")
    return userdir 

def CollectVendorInformation(file_name, fdate):
    
    """
    Takes a file path for a vendor profile page and extracts required information, like name, profile, etc.
    """
    
    logger.debug('processing: {}'.format(file_name))
    with open(file_name, encoding='utf8') as file_name:
        soup = BeautifulSoup(file_name, 'lxml')
    try:
        vendor = soup.find_all('div', attrs={'class', 'h1'})[0].get_text().strip()
    except:
        return
    # Aliases used by gwern
    if vendor != 'gwern' and vendor != 'chuck10':
        
        # ----- Query the database and check if the vendor is already there
        vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=="SR2", Vendors.vendor_name==vendor)).first()
        if not vendors:
            # ----- If not, determine the vendorSince data and save the vendor to the database
            # ----- Determines vendorSince by dating back the "joined 56 days ago" type information on the vendor page
            vendorSince_raw = soup.find_all('span', attrs={'class', 'container'})[0].get_text().strip()
            try:
                vendorSince = vendorSince_raw[vendorSince_raw.index("has been a vendor for")+21: vendorSince_raw.index("was last seen")]
            except:
                return
            if 'day' in vendorSince:
                vendorSince = vendorSince.replace("days","").replace("day","").replace("about","").strip()
                vendorSince = int(vendorSince)
            elif 'month' in vendorSince:
                vendorSince = vendorSince.replace("months","").replace("month","").replace("about","").strip()
                vendorSince = int(vendorSince)*30
            elif 'hours' in vendorSince:
                vendorSince = 0
            elif 'year' in vendorSince:
                vendorSince = vendorSince[vendorSince.index("about")+5: vendorSince.index("year")].strip()
                vendorSince = int(vendorSince)*365
            vendorSince = fdate - timedelta(days=vendorSince) 
              
            vendors = Vendors(marketplace_id='SR2', vendor_name=vendor, vendorSince=vendorSince)  
            session.add(vendors)  
            session.flush()
            session.refresh(vendors)
            session.commit() 
        else:
           session.commit() 
           
        # ----- Establish the vendor_id for further processing
        vendor_id = vendors.vendor_id
        
        # ----- Try to detect the PGP-key, if it is detected, try to find it in the database and save it, if it isn't
        try:
            pgp_key = soup.find_all('textarea', attrs={'class', 'container container_large'})[0].get_text().strip()
        except:
            pgp_key = None
            
        if pgp_key != None:
            keys = session.query(PGP_Keys).filter(and_(PGP_Keys.marketplace_id=='SR2', PGP_Keys.vendor_id==vendor_id, PGP_Keys.pgp_key == pgp_key)).first()  
            if keys == None:
                newpgp = PGP_Keys(marketplace_id='SR2', vendor_id=vendor_id, timestamp=fdate, pgp_key=pgp_key, source=file_name.name)  
                session.add(newpgp)  
                session.commit() 
                    
        
        # ----- Try to detect the vendor-profile, if it is detected, try to find it in the database and save it, if it isn't
        if soup.find_all('div', attrs={'class', 'user_content'}):    
             thisProfile = soup.find_all('div', attrs={'class', 'user_content'})[0].get_text().strip()
        else:  
            thisProfile = soup.find_all('div', attrs={'class', 'container container_large'})[0].get_text().strip()
        profiles = session.query(Vendorprofiles).filter(and_(Vendorprofiles.marketplace_id=='SR2', Vendorprofiles.vendor_id==vendor_id, Vendorprofiles.profile==thisProfile)).first()  
        if profiles == None:
            newProfile = Vendorprofiles(marketplace_id='SR2', vendor_id=vendor_id, timestamp=fdate, profile=thisProfile, source=file_name.name)  
            session.add(newProfile)  
            session.commit()   
        


def CollectCategories(category, file_name):
    
    """
    Takes a file path for a category page and extracts required information, like name, products, etc.
    """
    
    logger.debug('processing: {}'.format(file_name))
    with open(file_name, encoding='utf8') as file_name:
        soup = BeautifulSoup(file_name, 'lxml')
    if not soup.find(string=re.compile("no listings found")):
        collect = soup.find_all('div', attrs={'class', 'item'})
        # List of categories not to be splitted because they belong together, e.g. we want Drugs/Custom-Orders not Drugs/Custom/Orders
        names = ['biotic-materials', 'custom-orders', 'digital-goods', 'drug-paraphernalia', 'herbs-supplements', 'lotteries-games', 'lab-supplies', '4-homet', 'anit-rheumatic', 'pre-rolled', 'ah-7921', 'co-codamol']
        
        found = False
        for name in names:
            if name in category:
                cat1 = category[:category.index(name)]
                cat2 = category[category.index(name):category.index(name)+len(name)]
                cat3 = category[category.index(name)+len(name):]

                catArray = []
                if len(cat1) > 0:
                    if "-" in cat1:
                        cat1 = cat1.split("-")
                        for part in cat1:
                            part = part.capitalize()
                            if len(part)>0:
                                catArray.append(part)
                    else:
                        catArray.append(cat1)                            
                if len(cat2) > 0:
                    if "-" in cat2:
                        cat2 = cat2.split("-")
                        cat21 = ""
                        for part in cat2:
                            part = part.capitalize()
                            cat21 += part + " "
                        cat21 = cat21.strip()
                        catArray.append(cat21)
                    else:
                        catArray.append(cat2)
                if len(cat3) > 0:
                    if "-" in cat3:
                        cat3 = cat3.split("-") 
                        for part in cat3:
                            part = part.capitalize()
                            if len(part)>0:
                                catArray.append(part)
                    else:
                        catArray.append(cat3)
                found = True
            else:
                continue
        if found == False:
            catArray = category.split("-")            
            
        category_1 = catArray[0].capitalize()
        try: category_2= catArray[1].capitalize()
        except: category_2 = None
        try: category_3= catArray[2].capitalize()
        except: category_3 = None
        try: category_4= catArray[3].capitalize()
        except: category_4 = None
        try: category_5= catArray[4].capitalize()
        except: category_5 = None
        try: category_6= catArray[5].capitalize()
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

        # ----- Try to find the category in the database. If it is not found save it
        categories = session.query(Categories).filter(and_(Categories.marketplace_id=='SR2', Categories.category_1==category_1, Categories.category_2==category_2, Categories.category_3==category_3, Categories.category_4==category_4, Categories.category_5==category_5, Categories.category_6==category_6)).first()

        if not categories:
            categories = Categories(marketplace_id='SR2', category_1=category_1, category_2=category_2, category_3=category_3, category_4=category_4, category_5=category_5, category_6=category_6)  
            session.add(categories)  
            session.flush()
            session.refresh(categories)
            session.commit() 
        category_id = categories.category_id
        
        # ----- For this category page, go through all listed products and add the category to the product-entry in the database
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


def ReviewCore(reviews, thistable, check, deepcheck, fdate, product_id, vendor_id, filepath, srproductid, newestReviewDate):
    
    """
    Takes a block of reviews from the product page and processes it.
    This happens so often that it got it's own function
    """
    
    for i in range(1, reviews+1): 
        # ----- Forms 3-tuples of the form (evaluation, reviewtext, timestamp) 
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
        
        # ----- Finds the price of the product for the timestamp the review was left
        # Unfortunately that is as close as we get
        prices = session.query(Prices).filter(and_(Prices.marketplace_id=='SR2', Prices.srproductid==srproductid, Prices.timestamp<=timestamp)).order_by(desc(Prices.timestamp)).first()
        if prices!=None:
            reviewPrice = prices.price
        else:
            reviewPrice = session.query(Prices).filter(and_(Prices.marketplace_id=='SR2', Prices.srproductid==srproductid)).order_by(desc(Prices.timestamp)).first().price
        
        # ----- Checks if this particular review is already in the database
        if deepcheck:
            reviews = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.srproductid==srproductid, Reviews.reviewtext==reviewtext)).count()
            if reviews == 0:
                newReview = Reviews(marketplace_id='SR2', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, srproductid=srproductid)   
                session.add(newReview)  
                session.commit()
                
        # ----- Uses a rule of thumb to guess whether this review is already in the database
        if check:
            # ----- If the determined timestamp is is more recent than the newest review in the database, add it to the database
            if timestamp > newestReviewDate + timedelta(days=1):
                newReview = Reviews(marketplace_id='SR2', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, srproductid=srproductid)  
                session.add(newReview)  
                session.commit()
            # ----- Else: If it is in the vicinity of that newest date, there is a chance of the review not being there
            elif timestamp < newestReviewDate + timedelta(days=1) and timestamp > newestReviewDate - timedelta(days=1):
                # ----- Query the database based on (marketplace_id, vendor_id, product_id), and (reviewtext, evaluation date (+-1), evaluation)
                reviews = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.srproductid==srproductid, Reviews.timestamp > timestamp-timedelta(days=1), Reviews.timestamp < timestamp+timedelta(days=1),Reviews.reviewtext==reviewtext)).count()
                if reviews==0:
                    # ----- if there is no such review, add review to database
                    newReview = Reviews(marketplace_id='SR2', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, srproductid=srproductid)  
                    session.add(newReview)  
                    session.commit()
            else:
                pass
        elif not deepcheck:
            # ----- if neither a (simple) check nor a deep check was requested, add the review to the database
            newReview = Reviews(marketplace_id='SR2', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, srproductid=srproductid)   
            session.add(newReview)  
            session.commit()

        
def CollectProducts(pagelist, file_date):  
    
    """
    This is the main part of the script and it processes a product page
    """
    
    filepath = pagelist[0]
    # ----- Open first page of the collection (the main page)
    exact = False
    try:
        logger.debug('processing: {}'.format(pagelist[0]))
        with open(pagelist[0], encoding='utf8') as file_name:
            
            # ----- Establish date the file was created (i.e. the crawl took place). We want to document the timestamps as best as possible. Since crawls took place over a couple of days, simply taking the folder date is not good enough, since all reviewdata and backdating depends on the date
            # ----- For example if a review was left '56 days ago' and the folder says '2013-01-01' but the snapshot was really taken on 2013-01-03 then we might discard the review although it is worth saving
            # ----- Unfortunately, the file creation date does not correspond with the crawldate for dates before '2014-06-15'
            # ----- Thats why we only take it after that date
            if file_date > dateutil.parser.parse('2014-06-15'):
                times = datetime.fromtimestamp(os.path.getmtime(pagelist[0])).strftime('%Y-%m-%d %H:%M:%S')
                times = dateutil.parser.parse(times)
                if times < file_date + timedelta(days=7) and times > file_date - timedelta(days=7):
                    file_date = times
                exact = True
                
            soup = BeautifulSoup(file_name, 'lxml')
            if soup.find_all(id = 'content') and soup.find_all('h2'):  
    
                # ----- Vendor Class
                # ----- Find the vendors name on the page
                vendorcand = soup.find_all('h3')
                for h3 in vendorcand:
                    candidate = h3.get_text()
                    if 'vendor' in candidate:
                        vendor = list(h3.children)[1].get_text().strip()
                        break      
                # ----- Find out if the vendor is already in the database, if not save the new vendor
                vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='SR2', Vendors.vendor_name==vendor)).first()
                if not vendors:
                    vendors = Vendors(marketplace_id='SR2', vendor_name=vendor, vendorSince=file_date)  
                    session.add(vendors)  
                    session.flush()
                    session.refresh(vendors)
                    session.commit() 
                # ----- Establish the vendor_id for further processing
                vendor_id = vendors.vendor_id                
                
                # ----- Product Class    
                # ----- Find the product title and shipping information
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
                    
                # ----- Find the id used by Silkroad2 to identify products (unique identifier)                   
                # ----- There are several places to find it, all of which are not obvious
                srproductid = None
                try:
                    srproductid = soup.find_all('div', attrs={'class', 'item_image main'})[0]
                    srproductid = srproductid['id'].split("_")[1]
                except:
                    try:
                        srproductid = soup.find('div', attrs={'id':'footer'}).find_next("link")['href']
                        srproductid = srproductid[srproductid.index("/images/")+8:srproductid.index("|")]
                    except:
                        # ----- In the very rare case that the id was not found, there is no way to indentify this product
                        # ----- This only happens in the case of an HTML-error, for example only "half the page" was saved and the rest of the information is lost
                        return
                srproductid = int(srproductid)                
                
                
                # ----- Check if the product (with the unique SR2 id) already exists in the database  
                products = session.query(Products).filter(and_(Products.marketplace_id=='SR2', Products.srproductid==srproductid)).first()
                if not products:
                    # ----- Product is not in the database                       
                    products = Products(marketplace_id='SR2', name=product, vendor_id = vendor_id, vendor=vendor, category_id=None, category=None, origin=origin, destination=destination, last_seen = file_date, first_seen = file_date, date=file_date, source=pagelist[0], srproductid=srproductid)  
                    session.add(products)
                    session.flush()
                    session.refresh(products)
                    session.commit()                            
                else:
                    # ----- Product was found
                    # ----- Vendor is the same, update title, origin, destination, last_seen, date
                    if products.last_seen < file_date:
                        products.last_seen = file_date 
                    products.name = product
                    products.origin = origin
                    products.destination = destination
                    products.source = filepath
                    session.commit() 

                # ----- Establish the product_id for further processing                    
                product_id = products.product_id                           


                # ----- Price Class
                price = soup.find_all('div', attrs={'class', 'price_big'})[0].get_text().replace("฿","").strip()
                price = float(price)
                # ----- Get the latest price from the database
                prices = session.query(Prices).filter(and_(Prices.marketplace_id=='SR2', Prices.srproductid==srproductid)).order_by(desc(Prices.timestamp)).first()
                if prices == None:
                    # ----- If there is no price yet (at all), save this one
                    newPrice = Prices(marketplace_id='SR2', product_id=product_id, timestamp=file_date, price=price, vendor_id=vendor_id, source=filepath, srproductid=srproductid)  
                    session.add(newPrice)  
                    session.commit()  
                else:
                    if prices.price != price:
                        # ----- If the last price differs from this one, a change has taken place, save the price with the corresponding timestamp
                        newPrice = Prices(marketplace_id='SR2', product_id=product_id, timestamp=file_date, price=price, vendor_id=vendor_id, source=filepath, srproductid=srproductid)   
                        session.add(newPrice)
                        session.commit() 
                                
                # ----- Productprofile Class        
                thisProdProfile = soup.find_all('div', attrs={'class', 'container container_large'})[0].get_text().strip()
                profiles = session.query(Productprofiles).filter(and_(Productprofiles.marketplace_id=='SR2', Productprofiles.srproductid==srproductid , Productprofiles.profile==thisProdProfile)).first()  
                if profiles == None:
                    newProfile = Productprofiles(marketplace_id='SR2', vendor_id=vendor_id, product_id=product_id, timestamp=file_date, profile=thisProdProfile, source=filepath, srproductid=srproductid)                                                           
                    session.add(newProfile)  
                    session.commit() 
    
                # ----- Images Class        
                try:
                    thisImage = soup.find_all('div', attrs={'class', 'item_image main'})[0].get('id')
                except:
                    thisImage = None
                if thisImage != None:
                    images = session.query(Images).filter(and_(Images.marketplace_id=='SR2', Images.srproductid==srproductid, Images.filename==thisImage)).first()  
                    if images == None:
                        newImage = Images(marketplace_id='SR2', vendor_id=vendor_id, product_id=product_id, timestamp=file_date, filename=thisImage, source=filepath, srproductid=srproductid)                                                            
                        session.add(newImage) 
                        session.commit()
                        
        # ----- So far, we have evaluated the information that is the same on every page in the pagecollection 
        # ----- Whats different on each page is a list of reviews
        # ----- Since we are interested in those, we loop through those pages

        # ----- Pull newest reviewdate from the database
        reviews = session.query(Reviews).filter(and_(Reviews.marketplace_id=='SR2', Reviews.product_id==product_id)).order_by(desc(Reviews.timestamp)).first()
        newestReviewDate = None
        
        if reviews:
            newestReviewDate = reviews.timestamp
        else: 
            newestReviewDate = None
                
        for i in range(0,len(pagelist)):
            try:
                with open(pagelist[i], encoding='utf8') as file_name:                
                    soup = BeautifulSoup(file_name, 'lxml')
                    if soup.find_all(id = 'content') and soup.find_all('h2'):  
                        # ----- Determine the number of reviews on this page (reviews)
                        itemfeedbcand = soup.find_all('h3')
                        reviews = 0
                        try:
                            for cand in itemfeedbcand:
                                if "item feedback" in cand:
                                    thistable = cand.find_next('table')
                                    reviews = len(thistable.find_all('tr'))-1
                                    break
                        except:
                            continue
                    
                        if reviews != 0:
                            if newestReviewDate == None:           
                                # ----- add all of the reviews
                                ReviewCore(reviews, thistable, False, False, file_date, product_id, vendor_id, filepath, srproductid, newestReviewDate)
                                continue
                            elif exact == False:
                                # ----- If the date is not exact (not determined from file date), give the table to the function ReviewCore with check = False and deepcheck = True
                                ReviewCore(reviews, thistable, False, True, file_date, product_id, vendor_id, filepath, srproductid, newestReviewDate)
                            else:
                                # ----- Quick check: 1. Determine oldest and newest review on the page (oldestTimestamp, newestTimestamp)
                                timestamp = thistable.find_all('tr')[reviews].find_all('td')[2].get_text()
                                if len(timestamp.split(' ')) >2:
                                    raise NameError
                                else: 
                                    if timestamp == 'today':
                                        days = 0;
                                    else:
                                        days = int(timestamp.split(' ')[0])
                                        
                                oldestTimestamp = file_date - timedelta(days=days) 
                                
                                timestamp = thistable.find_all('tr')[1].find_all('td')[2].get_text()
                                if len(timestamp.split(' ')) >2:
                                    raise NameError
                                else: 
                                    if timestamp == 'today':
                                        days = 0;
                                    else:
                                        days = int(timestamp.split(' ')[0])
                                        
                                newestTimestamp = file_date - timedelta(days=days) 
                                # ----- Quick check: 2. If both of them are newer than the newest reviewdate in the database, add all of the reviews (call ReviewCore with check = False and deepCheck = False)
                                if oldestTimestamp > newestReviewDate + timedelta(days=1) and newestTimestamp > newestReviewDate + timedelta(days=1):  
                                    ReviewCore(reviews, thistable, False, False, file_date, product_id, vendor_id, filepath, srproductid, newestReviewDate)
                                # ----- Quick check: 3. If both of them are older than the newest reviewdate they habe most likely been added. We jump to the next page to save time.
                                elif oldestTimestamp < newestReviewDate - timedelta(days=1) and newestTimestamp < newestReviewDate - timedelta(days=1):
                                    continue
                                else:
                                    # ----- Quick check: 4. Else we pass the information on to ReviewCore with check = True and deepCheck = False
                                    ReviewCore(reviews, thistable, True, False, file_date, product_id, vendor_id, filepath, srproductid, newestReviewDate)
                                    
            except FileNotFoundError:
                logger.exception("FileNotFound: {}".format(pagelist[0]))  
            except IndexError:
                logger.exception("IndexError: {}".format(pagelist[0]))  

    except FileNotFoundError:
        logger.exception("FileNotFound: {}".format(pagelist[0]))            
    except NameError:
        logger.exception("NameError: {}".format(pagelist[0]))

        
def main(): 
    # ----- Switches to turn off certain parts of the script
    products = True
    categories = True
    vendors = True
    
    crawls = sorted(os.listdir(data_source))
    
    """
    # ----- For debugging: if the readout is stopped for example due to an error it can be resumed by entering the crawldate it failed on here:
    # ----- In case this option is used, replace the line "    for datestr in crawls:" with "    for datestr in crawls[ind:]:"

    # Find index in crawls
    for i in range(0,len(crawls)):
        if crawls[i]=='2014-11-05':
            ind = i
            break
    """

    for datestr in crawls:
        file_date = dateutil.parser.parse(datestr)
        file_path = os.path.join(data_source, datestr)
        if os.path.isdir(file_path):
            if products:
                if os.path.exists(os.path.join(file_path, "items")):
                    product_path = os.path.join(file_path, "items")
                    product_pages = sorted(os.listdir(product_path))
                    for i in range(0, len(product_pages)):
                        # ----- Make collections of pages that belong together, i.e productx, productx_feedback_page=2, etc.
                        pages =[]
                        pagenames =[]

                        if not "_feedback_page=" in product_pages[i]:
                            # ----- main page "leads" this collection
                            pages.append(os.path.join(product_path,product_pages[i])) 
                            pagenames.append(product_pages[i])

                            leading_page = product_pages[i].split('//')[-1]

                            for page in product_pages[i+1:]:
                                if "_feedback_page=" in page and leading_page == page[0:page.index('_')]:
                                    # ----- excluding vendor feedback pages (both vendor and item feedback can overlap, but this is taken care of)
                                    if page.count("_feedback_page=")==1 and not "_vendor_feedback_page" in page:
                                        pages.append(os.path.join(product_path, page))
                                        pagenames.append(page)
                                    elif page.count("_feedback_page=")==2:
                                        isThere = False
                                        for thisPage in pagenames:
                                            if thisPage == page[0: page.index("&vendor")]:
                                                isThere = True
                                                break
                                        if isThere == False:
                                            pages.append(os.path.join(product_path, page))
                                            pagenames.append(page[0:page.index("&vendor")])
                                else:
                                    break                            
                            # ----- Sort the list
                            try:
                                pages = sorted(pages, key=Order_key)
                            except:
                                continue
                            # ----- The pagelist containing relevant feedback data for the product at hand is passed to CollectProducts
                            CollectProducts(pages, file_date)

            if categories:
                if os.path.exists(os.path.join(file_path, "categories")):
                    category_path = os.path.join(file_path, "categories")
                    for cat in sorted(os.listdir(category_path)):
                        category_dir = os.path.join(category_path, cat)
                        if os.path.isdir(category_dir):
                            category_files = os.listdir(category_dir)
                            category_files = [os.path.join(category_dir, x) for x in category_files]
                            for file in category_files:
                                CollectCategories(cat, file, file_date)
                            
            if vendors:
                if os.path.exists(os.path.join(file_path, "users")):
                    user_path = os.path.join(file_path, "users")
                    for userpage in sorted(os.listdir(user_path)):
                        user_dir = os.path.join(user_path, userpage)
                        if not os.path.isdir(user_dir):
                            thisVendor = CleanVendorExpression(userpage)

                            try:
                                if lastVendor != thisVendor:
                                    lastVendor = thisVendor
                                    CollectVendorInformation(user_dir, file_date)
                            except:
                                lastVendor = thisVendor
                                CollectVendorInformation(user_dir, file_date)

if __name__=='__main__':
    start_time = time.time()
    try:
        main()
    except Exception:
        print("An Error occured...")
        traceback.print_exc()  
    print("--- %s seconds ---" % (time.time() - start_time))