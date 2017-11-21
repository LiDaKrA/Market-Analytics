# coding: utf-8
from bs4 import BeautifulSoup
import bs4
import time 
import dateutil.parser 
import os
import logging
import re
from datetime import timedelta, datetime
from model import *
import matplotlib.pyplot as plt
import traceback

try:
    session.rollback()
except:
    pass

# Data source down to the folder containing folders of format "2015-05-01"
data_source=""
# Directory for output files (like a file that contains errors encountered)
output_dir = ""

marketplace = session.query(Marketplaces).filter(Marketplaces.marketplace_id=='AGO').first()
if not marketplace:
    newmarketplace = Marketplaces(marketplace_id='AGO', marketplace_description="Agora", URL="http://i4rx33ibdndtqayh.onion", native_currency="BTC", active=False)  
    session.add(newmarketplace)  
    session.commit() 

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

#----- List all available Crawls Dates (in reverse order)
crawllistreverse = sorted(os.listdir(data_source), reverse=True)
crawllistreverse = [dateutil.parser.parse(x) for x in crawllistreverse]

#----- List all available Crawls Dates
crawllist = sorted(os.listdir(data_source))
crawllist = [x for x in crawllist if os.path.isdir(x)]
crawllist = [dateutil.parser.parse(x) for x in crawllist]



def DetermineDelayCurve(agoproductid):
    """
    This function is the zeroth part of a sequence that determines the next date (within the available crawls), that this product should be examined.
    It won't run automatically and is intended solely for experiments to set the values in def DetermineDate.
    This foundation for these experiments is a full crawl of the products data, not in any way abbreviated, since this would falsify the results.
    Pick a product_id and do a full crawl. Then run this function and set the values in def DetermineDate in the following way:
    (see external document with screenshots)
    """

    liste = []
    reviews = session.query(Reviews).filter(and_(Reviews.marketplace_id=='AGO', Reviews.agoproductid==agoproductid)).order_by(asc(Reviews.timestamp))
    startDate = list(reviews)[0].timestamp
    for i in range(14,(list(reviews)[-1].timestamp - list(reviews)[0].timestamp).days):
        curDate = startDate + timedelta(days=i)
        reviewcount = reviews.filter(and_(Reviews.timestamp>=curDate-timedelta(days=14), Reviews.timestamp<=curDate)).count()
        pred = reviews.filter(and_(Reviews.timestamp>=curDate, Reviews.timestamp<=curDate+timedelta(days=8))).count()
        liste.append([reviewcount, pred])
    plt.plot(*zip(*liste), marker='o', color='r', ls='')



def DetermineDate(reviewcount):
    """
    This function is the first part of a sequence that determines the next date (within the available crawls), that this product should be examined.
    Depending on the reviewcount of the last 7 days, determined prior to the function call via a database query, this function sets values for the duration until the next examination (in days).
    These Values were determined by experiments done with def DetermineDelayCurve.
    """
    return 1
    if reviewcount == 0:
        return 8
    elif reviewcount < 7:
        return 8
    else:
        return 8

def FindNextScan(fdate, nextscan):
    """
    This function is the second part of a sequence that determines the next date (within the available crawls), that this product should be examined.
    It takes in fdate, i.e. the date the file at hand was created and the nextscan-date, that was determined by def DetermineDate (the first part of the sequence).
    While this date is when we want to examine this product next, this won't always be possible, because crawls were only being done every few days.
    To handle this, we will find the latest date that lies between fdate and nextscan, and if that fails, we pick the earliest date after nextscan.
    """
    
    assert nextscan >= fdate
    
    """
    Taking care of demand peaks around 20/04
    if fdate >= dateutil.parser.parse("2014-04-15") and fdate <= dateutil.parser.parse("2014-06-16"):
        return 1
    """
    for i in range(0,len(crawllistreverse)):
        if nextscan >= crawllistreverse[i] and crawllistreverse[i]>=fdate:
            return crawllistreverse[i]

    # If that fails pick the closest after nextcrawl
    for i in range(0,len(crawllist)):
        if crawllist[i] >= nextscan:
            return crawllist[i]

def CollectCategories(page, fdate):
    logger.debug('processing: {}'.format(page))    
    with open(page, encoding='utf8') as page:  
        soup = BeautifulSoup(page, 'lxml') 
        if(soup.find(id="top-navigation")):
             # Category Class
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
        
            categories = session.query(Categories).filter(and_(Categories.marketplace_id=='AGO', Categories.category_1==category_1, Categories.category_2==category_2, Categories.category_3==category_3, Categories.category_4==category_4, Categories.category_5==category_5, Categories.category_6==category_6)).first()
        
            if not categories:
                categories = Categories(marketplace_id='AGO', category_1=category_1, category_2=category_2, category_3=category_3, category_4=category_4, category_5=category_5, category_6=category_6)  
                session.add(categories)  
                session.flush()
                session.refresh(categories)
                session.commit() 
            category_id = categories.category_id
            
            collect = soup.find_all('tr', attrs={'class', 'products-list-item'}) 
            
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
                        vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='AGO', Vendors.vendor_name==vendor)).first()
                        if not vendors:
                            vendors = Vendors(marketplace_id='AGO', vendor_name=vendor, rating=rating, deals=deals)  
                            session.add(vendors)  
                            session.flush()
                            session.refresh(vendors)
                            session.commit() 
                        #--------- Find the vendor_id for later reference
                        vendor_id = vendors.vendor_id 
                        
                        products = session.query(Products).filter(and_(Products.marketplace_id=='AGO', Products.agoproductid==agoproductid)).first()
                        if not products:
                            #--------- Product is not in the database                       
                            products = Products(marketplace_id='AGO', name=name, vendor_id = vendor_id, vendor=vendor, category_id=category_id, category=categ, origin=origin, destination=destination, last_seen = fdate, first_seen = fdate, date=fdate, source=page, agoproductid=agoproductid)  
                            session.add(products)
                            session.flush()
                            session.refresh(products)
                            session.commit()   
                except:
                    logger.exception("Page omitted: {}".format(page)) 

    

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)        
def CollectProducts(page, fdate): 
    filepath = page
    logger.debug('processing: {}'.format(page))
    with open(page, encoding='utf8') as fname:
        #--------- Establish date the file was created (i.e. the crawl took place)
        times = datetime.fromtimestamp(os.path.getmtime(page)).strftime('%Y-%m-%d %H:%M:%S')
        times = dateutil.parser.parse(times)
        #assert times < fdate + timedelta(days=7) and times > fdate - timedelta(days=7)
        fdate = times
        soup = BeautifulSoup(fname, 'lxml')
        if soup.find(id="single-product"):  
            # Vendor Class
            #--------- Find the vendors name on the page
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
            #--------- Find out if the vendor is already in the database
            #--------- if not make an entry for the new vendor
            vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='AGO', Vendors.vendor_name==vendor)).first()
            if not vendors:
                vendors = Vendors(marketplace_id='AGO', vendor_name=vendor, rating=rating, deals=deals)  
                session.add(vendors)  
                session.flush()
                session.refresh(vendors)
                session.commit() 
            #--------- Find the vendor_id for later reference
            vendor_id = vendors.vendor_id                

            
            # Category Class
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

            categories = session.query(Categories).filter(and_(Categories.marketplace_id=='AGO', Categories.category_1==category_1, Categories.category_2==category_2, Categories.category_3==category_3, Categories.category_4==category_4, Categories.category_5==category_5, Categories.category_6==category_6)).first()

            if not categories:
                categories = Categories(marketplace_id='AGO', category_1=category_1, category_2=category_2, category_3=category_3, category_4=category_4, category_5=category_5, category_6=category_6)  
                session.add(categories)  
                session.flush()
                session.refresh(categories)
                session.commit() 
            category_id = categories.category_id
            
            soup2 = soup.find(id='single-product')
            # Product Class    
            #--------- Find the product title and shipping information
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
                            
            #--------- Find the ID used by Agora to identify products 
            agoproductid = page.split('\\')[-1] 
            #--------- Look if product (with the SR2-internal ID) belonging to the same marketplace exists  
            products = session.query(Products).filter(and_(Products.marketplace_id=='AGO', Products.agoproductid==agoproductid)).first()
            if not products:
                #--------- Product is not in the database                       
                products = Products(marketplace_id='AGO', name=product, vendor_id = vendor_id, vendor=vendor, category_id=category_id, category=categ, origin=origin, destination=destination, last_seen = fdate, first_seen = fdate, date=fdate, source=page, agoproductid=agoproductid)  
                session.add(products)
                session.flush()
                session.refresh(products)
                session.commit()                            
            else:
                #---------  Product was found
                # Vendor is the same, update title, origin, destination, last_seen, date
                if products.last_seen < fdate:
                    products.last_seen = fdate 
                products.name = product
                products.origin = origin
                products.destination = destination
                products.source = filepath
                session.commit() 
                
            product_id = products.product_id                           
            #-------- product_id was established as a reference to this product

            #------ Price Class
            
            if soup2.find("div", attrs={'class','product-page-price'}):
                price = soup2.find("div", attrs={'class','product-page-price'}).get_text().replace("BTC","").strip()
            else:
                price = soup2.find('h1').next_sibling.next_sibling.get_text().replace("BTC","").strip()
        
            price = float(price)

            prices = session.query(Prices).filter(and_(Prices.marketplace_id=='AGO', Prices.agoproductid==agoproductid)).order_by(desc(Prices.timestamp)).first()
            if prices == None:                
                newPrice = Prices(marketplace_id='AGO', product_id=product_id, timestamp=fdate, price=price, vendor_id=vendor_id, source=filepath, agoproductid=agoproductid)  
                session.add(newPrice)  
                session.commit()  
            else:
                isThere = False
                if prices.price != price:
                    newPrice = Prices(marketplace_id='AGO', product_id=product_id, timestamp=fdate, price=price, vendor_id=vendor_id, source=filepath, agoproductid=agoproductid)   
                    session.add(newPrice)
                    session.commit() 


            #------ Productprofile Class 
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

            profiles = session.query(Productprofiles).filter(and_(Productprofiles.marketplace_id=='AGO', Productprofiles.agoproductid==agoproductid , Productprofiles.profile==profile)).first()  
            if profiles == None:
                newProfile = Productprofiles(marketplace_id='AGO', vendor_id=vendor_id, product_id=product_id, timestamp=fdate, profile=profile, source=filepath, agoproductid=agoproductid)                                                           
                session.add(newProfile)  
                session.commit() 

            #------ Images Class        
            try:
                thisImage = soup2.find('img').get('src')
                assert 'liabilities' in thisImage
                thisImage = thisImage.split("/")[-1]

            except:
                thisImage = None
            images = session.query(Images).filter(and_(Images.marketplace_id=='AGO', Images.agoproductid==agoproductid, Images.filename==thisImage)).first()  
            if images == None:
                newImage = Images(marketplace_id='AGO', vendor_id=vendor_id, product_id=product_id, timestamp=fdate, filename=thisImage, source=filepath, agoproductid=agoproductid)                                                            
                session.add(newImage) 
                session.commit()
            

            #---------  Pull newest reviewdate from the database
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
                            evaluation = soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr')[i].find('td').get_text()
                            evaluation = evaluation.replace('/5','').strip()
                                      
                            reviewtext = soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr')[i].find_all('td')[1].get_text()
                            reviewtext = reviewtext.replace(',','').replace(';','').replace("\n", "").replace("'", "")
                            reviewtext = removeNonAscii(reviewtext)
                            if len(soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr')[i].find_all('td')[2].get_text().split(" "))!=3:
                                raise NameError('Something wrong with the reviews')                                           
                            days = soup2.find('div', attrs={'class','embedded-feedback-list'}).find_all('tr')[i].find_all('td')[2].get_text().replace('days ago','').strip()
                            days = float(days)
                            timestamp = fdate - timedelta(days=days)
                        
                            prices = session.query(Prices).filter(and_(Prices.marketplace_id=='AGO', Prices.agoproductid==agoproductid, Prices.timestamp<=timestamp)).order_by(desc(Prices.timestamp)).first()
                            if prices!=None:
                                reviewPrice = prices.price
                            else:
                                reviewPrice = session.query(Prices).filter(and_(Prices.marketplace_id=='AGO', Prices.agoproductid==agoproductid)).order_by(desc(Prices.timestamp)).first().price
                            if newestReviewDate != None:
                                if timestamp > newestReviewDate + timedelta(days=1):
                                    newReview = Reviews(marketplace_id='AGO', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, agoproductid=agoproductid)  
                                    session.add(newReview)  
                                    session.commit()
                                    reviewcounter += 1
                                elif timestamp > newestReviewDate - timedelta(days=1):  
                                    # Query the database based on (marketplace_id, vendor_id, product_id), and (reviewtext, evaluation date (plus minus1), evaluation)
                                    reviews = session.query(Reviews).filter(and_(Reviews.marketplace_id=='AGO', Reviews.agoproductid==agoproductid, Reviews.evaluation==evaluation, Reviews.timestamp>=timestamp-timedelta(days=1), Reviews.timestamp<=timestamp+timedelta(days=1),Reviews.reviewtext==reviewtext)).count()
                                    if reviews==0:
                                        # add review to database
                                        newReview = Reviews(marketplace_id='AGO', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, agoproductid=agoproductid)  
                                        session.add(newReview)  
                                        session.commit()
                                        reviewcounter += 1
                            else:  
                                newReview = Reviews(marketplace_id='AGO', product_id=product_id, vendor_id=vendor_id, timestamp=timestamp, price=reviewPrice, evaluation=evaluation, reviewtext=reviewtext, source=filepath, agoproductid=agoproductid)  
                                session.add(newReview)  
                                session.commit()
                                reviewcounter += 1
                        except:
                            pass
                
                except FileNotFoundError:
                    print(traceback.print_exc())
                    logger.exception("FileNotFound: {}".format(page))  

def CollectVendorInformation(userdir, fdate):
    filepath = userdir
    logger.debug('processing: {}'.format(userdir))    
    vendor = userdir.split("/")[-1]   
    if vendor != 'agora':
        with open(userdir, encoding='utf8') as userdir:
            soup = BeautifulSoup(userdir, 'lxml') 
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
                        last_seen = fdate - timedelta(days=days)                         
                    elif "day" in last_seen:
                        #  Last seen 1 days 0 hours ago.
                        days = last_seen[:last_seen.index("day")].strip()
                        days = int(days)
                        last_seen = fdate - timedelta(days=days)
                    elif "hour" in last_seen:
                        last_seen = fdate
                    else:
                        raise NameError("Last_seen: other than Days")
                except AttributeError:
                    last_seen = None
                
                vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=="AGO", Vendors.vendor_name==vendor)).first()
                if not vendors:
                   vendors = Vendors(marketplace_id='AGO', vendor_name=vendor,last_seen = last_seen)  
                   session.add(vendors)  
                   session.flush()
                   session.refresh(vendors)
                   session.commit() 
                else:
                   session.commit() 
                   
                vendor_id = vendors.vendor_id
                
                if soup.find('div', attrs = {'class', 'vendor-verification'}):
                    verification = soup.find('div', attrs = {'class', 'vendor-verification'}).get_text()
                else:
                    verification = None
                ver = session.query(Verifications).filter(and_(Verifications.marketplace_id=='AGO', Verifications.vendor_id==vendor_id, Verifications.verification == verification)).first()  
                if ver == None:
                    newverification = Verifications(marketplace_id='AGO', vendor_id=vendor_id, timestamp=fdate, verification=verification, source=filepath)  
                    session.add(newverification)  
                    session.commit()                     
                    
    
                soup2 = soup.find('div', attrs = {'class', 'vendorbio-description'})
                if soup2.find('span', attrs = {'class', 'pgptoken'}):
                    pgp_key = soup2.find('span', attrs = {'class', 'pgptoken'}).get_text().strip()
                elif soup2.find('textarea', attrs = {'class', 'pgpkeytoken'}):
                    pgp_key = soup2.find('textarea', attrs = {'class', 'pgpkeytoken'}).get_text().strip()
                else:
                    pgp_key = None
        
                keys = session.query(PGP_Keys).filter(and_(PGP_Keys.marketplace_id=='AGO', PGP_Keys.vendor_id==vendor_id, PGP_Keys.pgp_key == pgp_key)).first()  
                if keys == None:
                    newpgp = PGP_Keys(marketplace_id='AGO', vendor_id=vendor_id, timestamp=fdate, pgp_key=pgp_key, source=filepath)  
                    session.add(newpgp)  
                    session.commit() 
                
                # Profile
                profile = soup.find('div', attrs = {'class', 'vendorbio-description'}).get_text()
                if pgp_key != None:
                    profile = profile[:profile.index(pgp_key[:20])]
        
                profiles = session.query(Vendorprofiles).filter(and_(Vendorprofiles.marketplace_id=='AGO', Vendorprofiles.vendor_id==vendor_id, Vendorprofiles.profile==profile)).first()  
                if profiles == None:
                    newProfile = Vendorprofiles(marketplace_id='AGO', vendor_id=vendor_id, timestamp=fdate, profile=profile, source=filepath)  
                    session.add(newProfile)  
                    session.commit()   

    
def main(): 
    products = True
    categories = False
    vendors = False
 
    crawls = sorted(os.listdir(data_source))
    crawls = [x for x in crawls if os.path.isdir(os.path.join(data_source,x))]

    """
    The following lines implement a naive approach to thinning out a dense dataset.
    Delay has to be set to a value. If uncommented there will by at least delay days between crawls.
    Example: The crawl on January, 1st was examined, delay is set to 3, then the crawl on January, 2nd will be ignored, but the crawl on January, 5th will be examined again.
    This fell out of favor after implementing product-wise delay, but is being kept here as a reference.
    """
    
    delay = 5
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
    This code allows to continue a read-out if it got interrupted.
    Determine the crawldate the read-out got interrupted by looking at certain timestamps in the database.
    Fill it in here and find the index in filteredcrawls / crawls
    Replace "for datestr in crawls" with "for datestr in crawls[ind:]" and uncomment the following lines.
    For debugging within a particular single crawl, just find the index and replace ... with "for datestr in crawls[ind:]"
    
    Find index in filteredcrawls
    for i in range(0,len(filteredcrawls)):
        if filteredcrawls[i]=='2014-10-16':
            ind = i
            break
    """
    for datestr in filteredcrawls:
        fdate = dateutil.parser.parse(datestr)
        tpath = os.path.join(data_source, datestr)
        if os.path.isdir(tpath):
            if products:
                if os.path.exists(os.path.join(tpath, "p")):
                    xpath = os.path.join(tpath, "p")
                    fsdir = sorted(os.listdir(xpath))
                    for page in fsdir:
                        pages = os.path.join(xpath,page)

                        CollectProducts(pages, fdate)
                        """
                        The following lines implement sparse examination of the data for purposes of saving computing power and time.
                        Apart from the price the variable we are interested in the most are reviews. That means reviewind products every 3 days, that have maybe two incomming reviews per week, is redundant.
                        Please read the documentation above to get an idea on how this will be done and decide if you want to do this.
                        It will save approx. 40% of time and leave no (or very little) datapoints on the table.
                        """
                        """
                        agoproductid = page.split('\\')[-1]     
                        try:
                            nextscan = session.query(Products).filter(Products.marketplace_id=='AGO',Products.agoproductid==agoproductid).first().nextScan  
                            if nextscan == None:
                                nextscan = fdate
                            if nextscan <= fdate:
                                CollectProducts(pages, fdate)
                        except:
                            CollectProducts(pages, fdate)
                        """

            if vendors:
                if os.path.exists(os.path.join(tpath, "vendor")):
                    xpath = os.path.join(tpath, "vendor")
                    for userpage in sorted(os.listdir(xpath)):
                        userdir = os.path.join(xpath, userpage)
                        if not os.path.isdir(userdir):
                            try:
                                CollectVendorInformation(userdir, fdate)
                            except:
                                logger.exception("Error occured in Vendor Information: {}".format(userdir))
                                logger.exception(traceback.print_exc())

            if categories:
                if os.path.exists(os.path.join(tpath, "cat")):
                    xpath = os.path.join(tpath, "cat")
                    for file in sorted(os.listdir(xpath)):
                        fpath = os.path.join(xpath, file)
                        if os.path.isdir(fpath):
                            for file in sorted(os.listdir(fpath)):
                                filepath = os.path.join(fpath,file)
                                try:
                                    CollectCategories(filepath, fdate)
                                except:
                                    logger.exception("Error occured in Category Information: {}".format(fpath))
                                    logger.exception(traceback.print_exc())
                        else:
                            try:
                                CollectCategories(fpath, fdate)
                            except:
                                logger.exception("Error occured in Category Information: {}".format(fpath))
                                logger.exception(traceback.print_exc())
                                
    start_time = time.time()
    try:
        main()
    except Exception:
        print("An Error occured...")
        traceback.print_exc()

    print("--- %s seconds ---" % (time.time() - start_time))
