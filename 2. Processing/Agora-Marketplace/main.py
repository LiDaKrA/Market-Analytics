# coding: utf-8
"""
---------------------------------------------------------------------
Description
---------------------------------------------------------------------
This script will be used to translate information from the Agora-Marketplace
from the Darknet-Market-Dataset published on https://www.gwern.net/DNM%20archives to a postgressql database.
After the data is fed into the database, various analysis can take place based on the then structured data.
---------------------------------------------------------------------
"""


server_mode = True

if server_mode:
    # ----- Make Changes here
    # Data source down to the folder containing folders of format "2015-05-01"
    data_source = ""
    # Directory for the logs
    output_dir = ""
else:
    # ----- and / or here
    # Data source down to the folder containing folders of format "2015-05-01"
    data_source = ""
    # Directory for the logs
    output_dir = ""
    
from bs4 import BeautifulSoup
import bs4
import time 
import dateutil.parser 
import os
import logging
import re
from datetime import timedelta, datetime
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

   
def CollectCategories(page, file_date):
    
    """
    Takes a file path for a category page and extracts required information, like name, products, etc.
    """
    
    logger.debug('processing: {}'.format(page))    
    with open(page, encoding='utf8') as page:  
        soup = BeautifulSoup(page, 'lxml') 
        if(soup.find(id="top-navigation")):
            # ----- Category Class
            categoryList = []
            subcategories = len(soup.find(id = 'top-navigation').find_all('a'))
            for i in range(0, subcategories):
                category = soup.find(id = 'top-navigation').find_all('a')[i].get_text()
                categoryList.append(category)
            
            categ = ""
            for category in categoryList:
                categ += category + "-"    
                category_1 = categoryList[0].capitalize()
                try: category_2= categoryList[1].capitalize()
                except: category_2 = None
                try: category_3= categoryList[2].capitalize()
                except: category_3 = None
                try: category_4= categoryList[3].capitalize()
                except: category_4 = None
                try: category_5= categoryList[4].capitalize()
                except: category_5 = None
                try: category_6= categoryList[5].capitalize()
                except: category_6 = None
            categ = categ[:-1]
        
            # ----- Try to find the category in the database. If it is not found save it
            categories = session.query(Categories).filter(and_(Categories.marketplace_id=='AGO', Categories.category_1==category_1, Categories.category_2==category_2, Categories.category_3==category_3, Categories.category_4==category_4, Categories.category_5==category_5, Categories.category_6==category_6)).first()
            if not categories:
                categories = Categories(marketplace_id='AGO', category_1=category_1, category_2=category_2, category_3=category_3, category_4=category_4, category_5=category_5, category_6=category_6)  
                session.add(categories)  
                session.flush()
                session.refresh(categories)
                session.commit() 
            category_id = categories.category_id
            
            collect = soup.find_all('tr', attrs={'class', 'products-list-item'}) 
            
            # ----- For this category page, go through all listed products and add the category to the product-entry in the database
            for product in collect:
                try:
                    agoproductid = product.find('a')['href'].split("/")[-1]
                    products = session.query(Products).filter(and_(Products.marketplace_id=='AGO', Products.agoproductid==agoproductid)).first()
                    if products:
                        name = product.find('a').get_text()
                        price = product.find_all('td')[2].get_text()
                        assert "BTC" in price
                        price = price.replace("BTC","")
                        price = float(price)
                        
                        if product.find('i', attrs = {'class', 'fa fa-arrow-right'}):
                            origin = product.find('i', attrs = {'class', 'fa fa-arrow-right'}).next_siblings
                            for sib in origin:
                                if isinstance(sib, bs4.element.NavigableString) and len(sib.strip())>0:
                                    origin = sib.strip()
                                    break
                        else:
                            origin = None
                        if product.find('i', attrs = {'class', 'fa fa-arrow-left'}):
                            destination = product.find('i', attrs = {'class', 'fa fa-arrow-left'}).next_siblings
                            for sib in destination:
                                if isinstance(sib, bs4.element.NavigableString) and len(sib.strip())>0:
                                    destination = sib.strip()
                                    break
                        else:
                            destination = None
                        vendor = product.find('a', attrs = {'class', 'gen-user-link'})['href'].split("/")[-1]
                        user_ratings = product.find('span', attrs = {'class', 'gen-user-ratings'}).get_text().strip()
                        try:
                            rating = user_ratings[:user_ratings.index(",")].strip()
                        except:
                            rating = None
                        try:
                            deals = user_ratings[user_ratings.index(",")+1:].strip()
                        except:
                            deals = user_ratings.strip()
                        deals = deals.replace("[","").replace("]","").replace("deals","").strip()
                        # ----- Check if the vendor is already in the database
                        vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='AGO', Vendors.vendor_name==vendor)).first()
                        if not vendors:
                            vendors = Vendors(marketplace_id='AGO', vendor_name=vendor, rating=rating, deals=deals)  
                            session.add(vendors)  
                            session.flush()
                            session.refresh(vendors)
                            session.commit() 
                        # ----- Determine the vendor_id for further processing
                        vendor_id = vendors.vendor_id 
                        # ----- Check if the product is already in the database
                        products = session.query(Products).filter(and_(Products.marketplace_id=='AGO', Products.agoproductid==agoproductid)).first()
                        if not products:
                            # ------ Product is not in the database                       
                            products = Products(marketplace_id='AGO', name=name, vendor_id = vendor_id, vendor=vendor, category_id=category_id, category=categ, origin=origin, destination=destination, last_seen = file_date, first_seen = file_date, date=file_date, source=page, agoproductid=agoproductid)  
                            session.add(products)
                            session.flush()
                            session.refresh(products)
                            session.commit()   
                except:
                    logger.exception("Page omitted: {}".format(page)) 

def CollectVendorInformation(file_path, file_date):
    
    """
    Takes a file path for a vendor profile page and extracts required information, like name, profile, etc.
    """
    
    filepath = file_path
    logger.debug('processing: {}'.format(file_path))    
    vendor = file_path.split("/")[-1]   
    # Alias used by gwern
    if vendor != 'agora':
        with open(file_path, encoding='utf8') as file_path:
            soup = BeautifulSoup(file_path, 'lxml') 
            if soup.find(id="top-navigation") and not soup.find(text=re.compile("Cannot find vendor by that name.")):
                user_ratings = soup.find('span', attrs = {'class', 'gen-user-ratings'}).get_text().strip()
                try:
                    rating = user_ratings[:user_ratings.index(",")].strip()
                except:
                    rating = None
                try:
                    deals = user_ratings[user_ratings.index(",")+1:].strip()
                except:
                    deals = user_ratings.strip()
                try:
                    last_seen = soup.find('div', attrs = {'class', 'vendorbio-stats-online'}).get_text()
                    if "Last seen" in last_seen:
                        last_seen = last_seen[last_seen.index("Last seen")+9:]
                    elif "On Vacation Mode" in last_seen:
                        last_seen = last_seen[last_seen.index("On Vacation Mode since")+22:]                        
                    else:
                        raise NameError("last_seen")
                    
                    if "month" in last_seen:
                        if "month" in last_seen and "day" in last_seen:
                            month = last_seen[:last_seen.index("month")].strip()
                            days = last_seen[last_seen.index(" ", last_seen.index("month")): last_seen.index("day")].strip()
                        else:    
                            month = last_seen[:last_seen.index("month")].strip()
                            days = "0"
                        month = int(month)
                        days = int(days)
                        days = days + month * 30
                        last_seen = file_date - timedelta(days=days)                         
                    elif "day" in last_seen:
                        #  Last seen 1 days 0 hours ago.
                        days = last_seen[:last_seen.index("day")].strip()
                        days = int(days)
                        last_seen = file_date - timedelta(days=days)
                    elif "hour" in last_seen:
                        last_seen = file_date
                    else:
                        raise NameError("Last_seen: other than Days")
                except AttributeError:
                    last_seen = None
                # ----- Query the database and check if the vendor is already there
                vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=="AGO", Vendors.vendor_name==vendor)).first()
                if not vendors:
                   vendors = Vendors(marketplace_id='AGO', vendor_name=vendor,last_seen = last_seen, deals = deals, rating = rating)  
                   session.add(vendors)  
                   session.flush()
                   session.refresh(vendors)
                   session.commit() 
                else:
                   session.commit() 
                # ----- Determine the vendor_id for further processing 
                vendor_id = vendors.vendor_id
                
                # ----- Determine the verification and add it to the database, if it is not there
                if soup.find('div', attrs = {'class', 'vendor-verification'}):
                    verification = soup.find('div', attrs = {'class', 'vendor-verification'}).get_text()
                else:
                    verification = None
                    
                ver = session.query(Verifications).filter(and_(Verifications.marketplace_id=='AGO', Verifications.vendor_id==vendor_id, Verifications.verification == verification)).first()  
                if ver == None:
                    newverification = Verifications(marketplace_id='AGO', vendor_id=vendor_id, timestamp=file_date, verification=verification, source=filepath)  
                    session.add(newverification)  
                    session.commit()                     
                    
                # ----- Determine the pgp-key and add it to the database, if it is not there    
                soup2 = soup.find('div', attrs = {'class', 'vendorbio-description'})
                if soup2.find('span', attrs = {'class', 'pgptoken'}):
                    pgp_key = soup2.find('span', attrs = {'class', 'pgptoken'}).get_text().strip()
                elif soup2.find('textarea', attrs = {'class', 'pgpkeytoken'}):
                    pgp_key = soup2.find('textarea', attrs = {'class', 'pgpkeytoken'}).get_text().strip()
                else:
                    pgp_key = None
        
                keys = session.query(PGP_Keys).filter(and_(PGP_Keys.marketplace_id=='AGO', PGP_Keys.vendor_id==vendor_id, PGP_Keys.pgp_key == pgp_key)).first()  
                if keys == None:
                    newpgp = PGP_Keys(marketplace_id='AGO', vendor_id=vendor_id, timestamp=file_date, pgp_key=pgp_key, source=filepath)  
                    session.add(newpgp)  
                    session.commit() 
                
                # ----- Determine the profile and add it to the database, if it is not there
                profile = soup.find('div', attrs = {'class', 'vendorbio-description'}).get_text()
                if pgp_key != None:
                    profile = profile[:profile.index(pgp_key[:20])]
        
                profiles = session.query(Vendorprofiles).filter(and_(Vendorprofiles.marketplace_id=='AGO', Vendorprofiles.vendor_id==vendor_id, Vendorprofiles.profile==profile)).first()  
                if profiles == None:
                    newProfile = Vendorprofiles(marketplace_id='AGO', vendor_id=vendor_id, timestamp=file_date, profile=profile, source=filepath)  
                    session.add(newProfile)  
                    session.commit()      

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)
        
def CollectProducts(page, file_date, datestr): 
    """
    This is the main part of the script and it processes a product page
    """
    
    filepath = page
    logger.debug('processing: {}'.format(page))
    with open(page, encoding='utf8') as fname:
        # ------ Establish date the file was created (i.e. the crawl took place)
        times = datetime.fromtimestamp(os.path.getmtime(page)).strftime('%Y-%m-%d %H:%M:%S')
        times = dateutil.parser.parse(times)
        #assert times < file_date + timedelta(days=7) and times > file_date - timedelta(days=7)
        file_date = times
        soup = BeautifulSoup(fname, 'lxml')
        if soup.find(id="single-product"):  
            # ------ Vendor Class
            # ------ Find the vendors name on the page
            try:
                vendor = soup.find('a', attrs = {'class', 'gen-user-link'}).next_sibling.strip().replace('"', r'')                 
            except:
                return
            if soup.find('span', attrs = {'class', 'gen-user-ratings'}):
                user_ratings = soup.find('span', attrs = {'class', 'gen-user-ratings'}).get_text().strip()
                try:
                    rating = user_ratings[:user_ratings.index(",")].strip()
                except:
                    rating = None
                try:
                    deals = user_ratings[user_ratings.index(",")+1:].strip()
                except:
                    deals = user_ratings.strip()
                deals = deals.replace("[","").replace("]","").replace("deals","").strip()
            else:
                rating = None
                deals = None
            # ------ Check if the vendor is already in the database
            vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='AGO', Vendors.vendor_name==vendor)).first()
            if not vendors:
                vendors = Vendors(marketplace_id='AGO', vendor_name=vendor, rating=rating, deals=deals)  
                session.add(vendors)  
                session.flush()
                session.refresh(vendors)
                session.commit() 
            # ------ Determine the vendor_id for further processing
            vendor_id = vendors.vendor_id                
            
            # ------ Category Class
            categoryList = []
            subcategories = len(soup.find(id = 'top-navigation').find_all('a'))
            for i in range(0, subcategories):
                category = soup.find(id = 'top-navigation').find_all('a')[i].get_text()
                categoryList.append(category)
            
            categ = ""
            for category in categoryList:
                categ += category + "-"    
                category_1 = categoryList[0].capitalize()
                try: category_2= categoryList[1].capitalize()
                except: category_2 = None
                try: category_3= categoryList[2].capitalize()
                except: category_3 = None
                try: category_4= categoryList[3].capitalize()
                except: category_4 = None
                try: category_5= categoryList[4].capitalize()
                except: category_5 = None
                try: category_6= categoryList[5].capitalize()
                except: category_6 = None
            categ = categ[:-1]

            # ----- Check if the category is already exists in the database  
            categories = session.query(Categories).filter(and_(Categories.marketplace_id=='AGO', Categories.category_1==category_1, Categories.category_2==category_2, Categories.category_3==category_3, Categories.category_4==category_4, Categories.category_5==category_5, Categories.category_6==category_6)).first()
            if not categories:
                categories = Categories(marketplace_id='AGO', category_1=category_1, category_2=category_2, category_3=category_3, category_4=category_4, category_5=category_5, category_6=category_6)  
                session.add(categories)  
                session.flush()
                session.refresh(categories)
                session.commit() 
            category_id = categories.category_id
            
            soup2 = soup.find(id='single-product')
            # ------ Product Class    
            # ------ Find the product title and shipping information
            product = removeNonAscii(soup2.find('h1').get_text().strip()).replace(";","").replace('"', r'').replace(",","")
            if soup2.find('div', attrs={'class','product-page-ships'}):
                soup3 = soup2.find('div', attrs={'class','product-page-ships'})
                soup3_string = soup3.get_text()
            
                origin = None
                destination = None
    
                if soup3.find('i', attrs = {'class','fa fa-arrow-right'}) or soup3.find('i', attrs = {'class','fa fa-arrow-left'}):
                    if soup3.find('i',attrs = {'class','fa fa-arrow-right'}):
                        if soup3.find('i',attrs = {'class','fa fa-arrow-right'}).parent.find('img', attrs={'class', 'flag-img'}):
                            origin = soup3.find('i',attrs = {'class','fa fa-arrow-right'}).parent.find('img', attrs={'class', 'flag-img'}).next_sibling.strip()
                        else:
                            origin = soup3.find('i',attrs = {'class','fa fa-arrow-right'}).next_sibling.strip()
                    if soup3.find('i',attrs = {'class','fa fa-arrow-left'}):
                        destination = soup3.find('i',attrs = {'class','fa fa-arrow-left'}).next_sibling.strip()
                    else:
                        destination = ""
                elif(('From:' in soup3_string) and ('To:' in soup3_string)):
                    origin = soup3_string[soup3_string.index("From:")+5: soup3_string.index("To:")-2].strip()
                    destination = soup3_string[soup3_string.index("To:")+3: len(soup3_string)].strip()          
                elif 'From' in soup3_string:
                    origin = soup3_string[soup3_string.index("From")+5: len(soup3_string)-1].strip()
                elif 'To' in soup3_string:
                    origin = soup3_string[soup3_string.index("To")+5: len(soup3_string)-1].strip()
                elif len(soup3_string.strip())==0:
                    pass
                else:
                    raise NameError('origin/destination')  
                if origin != None:
                    origin = origin.replace(";","").replace(";)","").replace("Ships from","").strip()
                if destination != None:
                    destination = destination.replace(";","").replace(";)","").replace("Ships to","").strip()
            else:
                origin = None
                destination = None                
                            
            # ------ Find the id used by Agora to identify products (unique identifier) 
            agoproductid = page.split('/')[-1]
            # ------ Look if product (with the SR2-internal ID) belonging to the same marketplace exists  
            products = session.query(Products).filter(and_(Products.marketplace_id=='AGO', Products.agoproductid==agoproductid)).first()
            if not products:                       
                products = Products(marketplace_id='AGO', name=product, vendor_id = vendor_id, vendor=vendor, category_id=category_id, category=categ, origin=origin, destination=destination, last_seen = file_date, first_seen = file_date, date=file_date, source=page, agoproductid=agoproductid)  
                session.add(products)
                session.flush()
                session.refresh(products)
                session.commit()                            
            else:
                # ------ Product was found: Update values
                # ------ Vendor is the same, update title, origin, destination, last_seen, date
                if products.last_seen < file_date:
                    products.last_seen = file_date 
                products.name = product
                products.origin = origin
                products.destination = destination
                products.source = filepath
                session.commit() 
            
            # ------ Determine the product_id for further processing
            product_id = products.product_id                           

            #------ Price Class
            if soup2.find("div", attrs={'class','product-page-price'}):
                price = soup2.find("div", attrs={'class','product-page-price'}).get_text().replace("BTC","").strip()
            else:
                price = soup2.find('h1').next_sibling.next_sibling.get_text().replace("BTC","").strip()
        
            price = float(price)

            # ------ Check if the current price differs from the current price in the database 
            prices = session.query(Prices).filter(and_(Prices.marketplace_id=='AGO', Prices.agoproductid==agoproductid)).order_by(desc(Prices.timestamp)).first()
            if prices == None:                
                newPrice = Prices(marketplace_id='AGO', product_id=product_id, timestamp=file_date, price=price, vendor_id=vendor_id, source=filepath, agoproductid=agoproductid)  
                session.add(newPrice)  
                session.commit()  
            else:
                if prices.price != price:
                    newPrice = Prices(marketplace_id='AGO', product_id=product_id, timestamp=file_date, price=price, vendor_id=vendor_id, source=filepath, agoproductid=agoproductid)   
                    session.add(newPrice)
                    session.commit() 


            # ------ Productprofile Class 
            profs = soup2.find('a', attrs = {'class', 'gen-user-link'}).previous_siblings
            profs = list(profs)
            profs = reversed(profs)
            profs = list(profs)           
            profile = ""
            for i in range(0,len(profs)-1):
                try:
                    profs[i].get_text()
                except:
                    profile += profs[i].strip() + "\n"
            profile = profile[:profile.rindex("Brought to you by:")].strip()
            
            
            # ------ Check if the productprofile is already exists in the database 
            profiles = session.query(Productprofiles).filter(and_(Productprofiles.marketplace_id=='AGO', Productprofiles.agoproductid==agoproductid , Productprofiles.profile==profile)).first()  
            if profiles == None:
                newProfile = Productprofiles(marketplace_id='AGO', vendor_id=vendor_id, product_id=product_id, timestamp=file_date, profile=profile, source=filepath, agoproductid=agoproductid)                                                           
                session.add(newProfile)  
                session.commit() 

            # ------ Images Class        
            try:
                thisImage = soup2.find('img').get('src')
                assert 'liabilities' in thisImage
                thisImage = thisImage.split("/")[-1]

            except:
                thisImage = None
            # ----- Check if the image is already exists in the database 
            images = session.query(Images).filter(and_(Images.marketplace_id=='AGO', Images.agoproductid==agoproductid, Images.filename==thisImage)).first()  
            if images == None:
                newImage = Images(marketplace_id='AGO', vendor_id=vendor_id, product_id=product_id, timestamp=file_date, filename=thisImage, source=filepath, agoproductid=agoproductid)                                                            
                session.add(newImage) 
                session.commit()
                
            # ------ Due to some HTML inconsistencies priot to "2014-07-26" a not-so-elegant procedure is being done
            if dateutil.parser.parse(datestr) < dateutil.parser.parse("2014-07-26"):
                rawdatainput = True
                replacedates = False
            elif dateutil.parser.parse(datestr) == dateutil.parser.parse("2014-07-26"):
                rawdatainput = False
                replacedates = True 
            else:
                rawdatainput = False
                replacedates = False
            
            if not rawdatainput and not replacedates:
                # -----  Pull newest reviewdate from the database
                reviews = session.query(Reviews).filter(and_(Reviews.marketplace_id=='AGO', Reviews.agoproductid==agoproductid)).order_by(desc(Reviews.timestamp)).first()
                newestReviewDate = None
            
                if reviews:
                    newestReviewDate = reviews.timestamp
                else: 
                    newestReviewDate = None
            reviewcounter = 0
            if soup2.find('div', attrs={'class','embedded-feedback-list'}):  
                try:
                    reviews = len(soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr'))
    
                    for i in range(0, reviews): 
                        try:
                            # ------ Form tuples of the form (evaluation, reviewtext, timestamp)
                            evaluation = soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr')[i].find('td').get_text()
                            evaluation = evaluation.replace('/5','').strip()
                                      
                            reviewtext = soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr')[i].find_all('td')[1].get_text()
                            reviewtext = reviewtext.replace(',','').replace(';','').replace("\n", "").replace("'", "")
                            reviewtext = removeNonAscii(reviewtext)
                            if len(soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr')[i].find_all('td')[2].get_text().split(" "))!=3:
                                raise NameError('Something wrong with the reviews')                                           
                            days = soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr')[i].find_all('td')[2].get_text().replace('days ago','').strip()
                            days = float(days)
                            timestamp = file_date - timedelta(days=days)
                        
                            # ----- Query the price for this product at the time
                            prices = session.query(Prices).filter(and_(Prices.marketplace_id=='AGO', Prices.agoproductid==agoproductid, Prices.timestamp<=timestamp)).order_by(desc(Prices.timestamp)).first()
                            if prices != None:
                                reviewPrice = prices.price
                            else:
                                reviewPrice = session.query(Prices).filter(and_(Prices.marketplace_id=='AGO', Prices.agoproductid==agoproductid)).order_by(desc(Prices.timestamp)).first().price
                            if not rawdatainput and not replacedates:
                                if newestReviewDate != None:
                                    if timestamp > newestReviewDate + timedelta(days=2):
                                        newReview = Reviews(marketplace_id='AGO', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, agoproductid=agoproductid)  
                                        session.add(newReview)  
                                        session.commit()
                                        reviewcounter += 1
                                    elif timestamp > newestReviewDate - timedelta(days=2):  
                                        # ----- Query the database based on (marketplace_id, vendor_id, product_id), and (reviewtext, evaluation date (plus minus1), evaluation)
                                        reviews = session.query(Reviews).filter(and_(Reviews.marketplace_id=='AGO', Reviews.agoproductid==agoproductid, Reviews.evaluation==evaluation, Reviews.timestamp>=timestamp-timedelta(days=2), Reviews.timestamp<=timestamp+timedelta(days=2),Reviews.reviewtext==reviewtext)).count()
                                        if reviews==0:
                                            # ----- add review to database
                                            newReview = Reviews(marketplace_id='AGO', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, agoproductid=agoproductid)  
                                            session.add(newReview)  
                                            session.commit()
                                            reviewcounter += 1
                                else:  
                                    newReview = Reviews(marketplace_id='AGO', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, agoproductid=agoproductid)  
                                    session.add(newReview)  
                                    session.commit()
                                    reviewcounter += 1
                            if rawdatainput or replacedates:
                                reviews = session.query(Reviews).filter(and_(Reviews.marketplace_id=='AGO', Reviews.agoproductid==agoproductid, Reviews.evaluation==evaluation, Reviews.reviewtext==reviewtext)).first()
                                if reviews == None:
                                    # ----- add new review to database
                                    newReview = Reviews(marketplace_id='AGO', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, agoproductid=agoproductid)
                                    session.add(newReview)  
                                    session.commit()
                                if replacedates and reviews != None:
                                    # ------ Add the correct date to the review entries (modify the database entry)
                                    reviews.timestamp = timestamp
                                    reviews.price=reviewPrice
                                    reviews.source=filepath
                                    session.commit() 
                                reviewcounter += 1
                        except:
                            print(traceback.print_exc())
                            pass
                except FileNotFoundError:
                    logger.exception("FileNotFound: {}".format(page))  
                    print(traceback.print_exc())




    
def main(): 
    # ----- Switches to turn off certain parts of the script
    products = True
    categories = True
    vendors = True
 
    crawls = sorted(os.listdir(data_source))
    crawls = [x for x in crawls if os.path.isdir(os.path.join(data_source,x))]

    """
    The following lines implement a naive approach to thinning out a dense dataset.
    Delay has to be set to a value. If uncommented there will by at least delay days between crawls.
    Example: The crawl on January, 1st was examined, delay is set to 3, then the crawl on January, 2nd will be ignored, but the crawl on January, 5th will be examined again.
    This fell out of favor after implementing product-wise delay, but is being kept here as a reference.
    """
    delay = 3
    filteredcrawls = []

    filteredcrawls.append(crawls[0])
    lastCrawl = dateutil.parser.parse(crawls[0])

    for date in crawls[1:]:
        currentCrawl = dateutil.parser.parse(date)
        if (currentCrawl - lastCrawl).days < delay:
            pass
        else:
            filteredcrawls.append(date)
            lastCrawl = currentCrawl
    """
    # ----- For debugging: if the readout is stopped for example due to an error it can be resumed by entering the crawldate it failed on here:
    # ----- In case this option is used, replace the line "for datestr in filteredcrawls:" with "    for datestr in crawls[ind:]:"

    # Find index in crawls
    for i in range(0,len(crawls)):
        if crawls[i]=='2014-11-05':
            ind = i
            break
    """
    
    for datestr in filteredcrawls:
        file_date = dateutil.parser.parse(datestr)
        file_path = os.path.join(data_source, datestr)
        if os.path.isdir(file_path):
            if products:
                if os.path.exists(os.path.join(file_path, "p")):
                    product_path = os.path.join(file_path, "p")
                    product_directory = sorted(os.listdir(product_path))
                    for page in product_directory:
                        pages = os.path.join(product_path, page)
                        try:
                            CollectProducts(pages, file_date, datestr)
                        except:
                            logger.exception("Error occured in CollectProducts: {}".format(pages))
                            logger.exception(traceback.print_exc())
            if vendors:
                if os.path.exists(os.path.join(file_path, "vendor")):
                    vendor_path = os.path.join(file_path, "vendor")
                    for userpage in sorted(os.listdir(vendor_path)):
                        userdir = os.path.join(vendor_path, userpage)
                        if not os.path.isdir(userdir):
                            try:
                                CollectVendorInformation(userdir, file_date)
                            except:
                                logger.exception("Error occured in CollectVendorInformation: {}".format(userdir))
                                logger.exception(traceback.print_exc())

            if categories:
                if os.path.exists(os.path.join(file_path, "cat")):
                    category_path = os.path.join(file_path, "cat")
                    for file in sorted(os.listdir(category_path)):
                        fpath = os.path.join(category_path, file)
                        if os.path.isdir(fpath):
                            for file in sorted(os.listdir(fpath)):
                                filepath = os.path.join(fpath,file)
                                try:
                                    CollectCategories(filepath, file_date)
                                except:
                                    logger.exception("Error occured in CollectCategories (1): {}".format(fpath))
                                    logger.exception(traceback.print_exc())

                        else:
                            try:
                                CollectCategories(fpath, file_date)
                            except:
                                logger.exception("Error occured in CollectCategories (2): {}".format(fpath))
                                logger.exception(traceback.print_exc())

if __name__=='__main__':
    start_time = time.time()
    try:
        main()
    except Exception:
        print("An Error occured...")
        traceback.print_exc()
    print("--- %s seconds ---" % (time.time() - start_time))
