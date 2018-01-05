# coding: utf-8

"""
---------------------------------------------------------------------
Description
---------------------------------------------------------------------
This script will be used to translate information from the Dogeroad-Marketplace
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
from datetime import timedelta
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

    
def CollectProducts(page, fdate): 
    
    """
    This is the main part of the script and it processes a product page
    """
    
    logger.debug('processing: {}'.format(page))
    
    filepath = page
    with open(filepath, encoding='utf8') as fname:
        soup = BeautifulSoup(fname, "lxml")
        
        # ----- Making sure the page is not empty
        if soup.find_all('div', attrs={'class','itemInfo'}):
            # ----- Vendor Class
            # ----- Find the vendor name
            vendor = soup.find_all('p', attrs={'class','vendor'})[0].find_all('a')[0].get_text().strip()
            drvendorid = soup.find_all('p', attrs={'class','vendor'})[0].find_all('a')[0]['href'].split('/')[-1]
            
            # ----- Check if the vendor is already in the database
            #--------- if not make an entry for the new vendor
            vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=='DOR', Vendors.drvendorid==drvendorid)).first()
            if not vendors:
                # ----- If not, add the vendor to the database
                vendors = Vendors(marketplace_id='DOR', vendor_name=vendor, drvendorid=drvendorid)  
                session.add(vendors)  
                session.flush()
                session.refresh(vendors)
                session.commit() 
                
            # ----- Determine the vendor_id for further processing
            vendor_id = vendors.vendor_id

            # ----- Product Class
            # ----- Determine the product name
            product = soup.find_all('div', attrs={'class','itemInfo'})[0].find_all('h2')[0].get_text().strip()
            
            # ----- Filtering information on Dogeroad is a little tricky
            listsib = []
            string = ""
            for sibling in soup.find_all('div', attrs={'class','price'})[0].next_siblings:
                listsib.append(sibling)
            # ----- Excluding the id = main
            for i in range(0, len(listsib)-2):
                string = string + str(listsib[i]).replace('<br/>','').replace('<br/>','').strip()
            
            if 'From' in string and 'To:' in string:
                origin = string[string.index('From:')+5:string.index("Ship's To")].strip()
                destination = string[string.index("Ship's To")+10:].strip()
            elif 'From' in string:
                origin = string[string.index('From:')+5:].strip()
                destination = ""
            elif 'To' in string:  
                origin = ""
                destination = string[string.index("Ship's To")+10:].strip()
            else:
                raise NameError('origin/destination')
            if ';' in origin:
                origin = origin.replace(";",",")
            if ';' in destination:
               destination = destination.replace(";",",")
                        
            # ----- Find the Dogeroad product_id (unique identifier)                    
            drproductid = soup.find_all('div', attrs={'class', 'itemInfo'})[0]['id'].replace("prod_","")             
            
            # ----- Check if that product is already in the database  
            products = session.query(Products).filter(and_(Products.marketplace_id=='DOR', Products.drproductid==drproductid)).first()
            if not products:
                # ----- if it is not, add it                       
                products = Products(marketplace_id='DOR', name=product, vendor_id = vendor_id, vendor=vendor, category_id=None, category=None, origin=origin, destination=destination, last_seen = fdate, first_seen = fdate, date=fdate, source=filepath, drproductid=drproductid)  
                session.add(products)
                session.flush()
                session.refresh(products)
                session.commit()                            
            else:
                # ----- Update the information
                # ------ Vendor is the same, update title, origin, destination, last_seen, date
                if products.last_seen < fdate:
                    products.last_seen = fdate 
                products.name = product
                products.origin = origin
                products.destination = destination
                products.source = filepath
                session.commit() 
            
            # ----- Determine the product_id for further processing
            product_id = products.product_id              
            
    
            
            # ----- Price Class            
            # ----- Determine the price of the product
            price = soup.find_all('span', attrs={'class','priceValue'})[0].get_text()
            if "DOGE" in price:
                price = float(price.replace("DOGE","").replace(",","."))
            # ----- Check if the price is already there, if it has changes, document it
            prices = session.query(Prices).filter(and_(Prices.marketplace_id=='DOR', Prices.drproductid==drproductid)).order_by(desc(Prices.timestamp)).first()
            if prices == None:
                newPrice = Prices(marketplace_id='DOR', product_id=product_id, timestamp=fdate, currency = 'DOGE', price=price, vendor_id=vendor_id, source=filepath, drproductid=drproductid)  
                session.add(newPrice)  
                session.commit()  
            else:
                if prices.price != price:
                    newPrice = Prices(marketplace_id='DOR', product_id=product_id, timestamp=fdate, currency = 'DOGE', price=price, vendor_id=vendor_id, source=filepath, drproductid=drproductid)   
                    session.add(newPrice)
                    session.commit() 

            
            # ----- Productprofile Class        
            thisProdProfile = soup.find_all(id = 'main')[0].get_text().strip()
            profiles = session.query(Productprofiles).filter(and_(Productprofiles.marketplace_id=='DOR', Productprofiles.drproductid==drproductid , Productprofiles.profile==thisProdProfile)).first()  
            if profiles == None:
                newProfile = Productprofiles(marketplace_id='DOR', vendor_id=vendor_id, product_id=product_id, timestamp=fdate, profile=thisProdProfile, source=filepath, drproductid=drproductid)                                                           
                session.add(newProfile)  
                session.commit() 
                      
            # ----- Images Class        
            thisImage = soup.find_all(id = 'item_listing')[0].find_all('img')
            for image in thisImage:
                imagename = image.parent['href'].split("/")[-1]
                
                images = session.query(Images).filter(and_(Images.marketplace_id=='DOR', Images.drproductid==drproductid, Images.filename==imagename)).first()  
                if images == None:
                    newImage = Images(marketplace_id='DOR', vendor_id=vendor_id, product_id=product_id, timestamp=fdate, filename=imagename, source=filepath, drproductid=drproductid)                                                            
                    session.add(newImage) 
                    session.commit()


def CollectVendorInformation(userpage, file_date):
    """
    Takes a file path for a vendor profile page and extracts required information, like name, profile, etc.
    """
    logger.debug('processing: {}'.format(userpage))
    pagename = userpage
    with open(userpage, encoding='utf8') as userpage:
        soup = BeautifulSoup(userpage, 'lxml')

        # ----- Determine the vendor_id
        drvendorid = pagename.split('\\')[-1]
        # ----- Determine the vendorname
        vendor = soup.find('h2').get_text().strip()
        # ----- Determine the location, and vendorSince
        allrows = soup.find_all('div', attrs={'class', 'row-fluid'})
        for row in allrows[2:]:
            if row.find('div', attrs={'class', 'span2'}).get_text() == "Location":
                location = row.find('div', attrs={'class', 'span7'}).get_text()
            if row.find('div', attrs={'class', 'span2'}).get_text() == "Registered":
                vendorSince = row.find('div', attrs={'class', 'span7'}).get_text()
                if 'day' in vendorSince:
                    vendorSince = vendorSince.replace("days ago","").replace("day ago","").strip()
                    vendorSince = int(vendorSince)
                    vendorSince = file_date - timedelta(days=vendorSince)
                elif 'month' in vendorSince:
                    vendorSince = vendorSince.replace("months","").replace("month","").replace("about","").strip()
                    vendorSince = int(vendorSince)*30
                elif 'hours' in vendorSince:
                    vendorSince = file_date
                elif 'year' in vendorSince:
                    # This never happens
                    raise NameError("Take care of vendorSince : year")
                else:
                    vendorSince = dateutil.parser.parse(vendorSince)

            if row.find('div', attrs={'class', 'span2'}).get_text() == "PGP Fingerprint":
                pgp_fingerprint = row.find('div', attrs={'class', 'span7'}).get_text()               
            if row.find('div', attrs={'class', 'span2'}).get_text() == "PGP Public Key":
                pgp_key = row.find('pre', attrs={'id', 'span9 well'}).get_text()              
        
        # ----- Check if the vendor is in the database
        vendors = session.query(Vendors).filter(and_(Vendors.marketplace_id=="DOR", Vendors.vendor_name==vendor)).first()
        if not vendors:
           # ----- Add the vendor
           vendors = Vendors(marketplace_id='DOR', vendor_name=vendor, drvendorid=drvendorid, vendorSince=vendorSince, location=location)  
           session.add(vendors)  
           session.flush()
           session.refresh(vendors)
           session.commit() 
        else:
            # ----- Update information
           vendors.drvendorid = drvendorid
           vendors.vendorSince = vendorSince
           vendors.location = location
           session.commit()
        # ----- Determine the vendor_id for further processing
        vendor_id = vendors.vendor_id
    
        # ----- Check if the pgp-key is already in the database    
        keys = session.query(PGP_Keys).filter(and_(PGP_Keys.marketplace_id=='DOR', PGP_Keys.vendor_id==vendor_id, PGP_Keys.pgp_key == pgp_key)).first()  
        if keys == None:
            # ----- Add the pgp-key
            newpgp = PGP_Keys(marketplace_id='DOR', drvendorid=drvendorid, vendor_id=vendor_id, timestamp=file_date, pgp_key=pgp_key, source=pagename)  
            session.add(newpgp)  
            session.commit() 
    
        # ----- Check if the pgp-fingerprint is already in the database
        fingerprint = session.query(PGP_Fingerprints).filter(and_(PGP_Fingerprints.marketplace_id=='DOR', PGP_Fingerprints.vendor_id==vendor_id, PGP_Fingerprints.pgp_fingerprint == pgp_fingerprint)).first()  
        if fingerprint == None:
            # ----- add the pgp-fingerprint
            pgp_fingerprint = PGP_Fingerprints(marketplace_id='DOR', drvendorid=drvendorid, vendor_id=vendor_id, timestamp=file_date, pgp_fingerprint=pgp_fingerprint, source=pagename)  
            session.add(pgp_fingerprint)  
            session.commit() 


def CollectCategories(file_name, file_date):
    
    """
    Takes a file path for a category page and extracts required information, like name, products, etc.
    """
    
    logger.debug('processing: {}'.format(file_name))
    with open(file_name, encoding='utf8') as file_name:
        soup = BeautifulSoup(file_name, 'lxml')
        category = soup.find('h2').get_text().replace("Category: ", "").strip()
        
        # ----- Check if the category is already in the database
        categories = session.query(Categories).filter(and_(Categories.marketplace_id=='DOR', Categories.category==category)).first()
        if not categories:
            # ----- Add the category
            categories = Categories(marketplace_id='DOR', category=category)  
            session.add(categories)  
            session.flush()
            session.refresh(categories)
            session.commit() 
        category_id = categories.category_id
                
        collect = soup.find_all('div', attrs={'class', 'itemImg'}) 
        # ----- loop through all entries in the page and add the category to the products
        for child in collect:
            drproductid = child.find('a')['href'].split("/")[-1]
            products = session.query(Products).filter(and_(Products.marketplace_id=='DOR', Products.drproductid==drproductid)).first()
            products.category_id = category_id 
            products.category = category
            session.commit()
        
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
                if os.path.exists(os.path.join(file_path, "item")):
                    product_path = os.path.join(file_path, "item")
                    product_dir = sorted(os.listdir(product_path))
                    for i in range(0, len(product_dir)):
                        fpath = os.path.join(product_path, product_dir[i])
                        try:
                            CollectProducts(fpath, file_date)
                        except:
                            logger.exception("Error occured in CollectProducts: {}".format(fpath))
                            logger.exception(traceback.print_exc())
                     
            if vendors:
                if os.path.exists(os.path.join(file_path, "user")):
                    vendor_path = os.path.join(file_path, "user")
                    vendor_dir = sorted(os.listdir(vendor_path))
                    for i in range(0, len(vendor_dir)):
                        fpath = os.path.join(vendor_path, vendor_dir[i])
                        try:
                            CollectVendorInformation(fpath, file_date) 
                        except:
                            logger.exception("Error occured in CollectVendorInformation: {}".format(fpath))
                            logger.exception(traceback.print_exc())

            if categories:
                if os.path.exists(os.path.join(file_path, "category")):
                    category_path = os.path.join(file_path, "category")
                    category_dir = sorted(os.listdir(category_path))
                    for i in range(0, len(category_dir)):
                        fpath = os.path.join(category_path, category_dir[i])
                        try:
                            CollectCategories(fpath, file_date)
                        except:
                            logger.exception("CollectCategories: {}".format(fpath))
                            logger.exception(traceback.print_exc())



if __name__=='__main__':
    start_time = time.time()
    try:
        main()
    except:
        print("An Error occured...")
        traceback.print_exc()
    print("--- %s seconds ---" % (time.time() - start_time))