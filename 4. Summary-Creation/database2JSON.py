# coding: utf-8
# Postgres to JSON Converter

from sqlalchemy import func
import time
import numpy as np
from amodel import *
import json
import pickle
import traceback

math = 'agora'

if math == 'agora':
    output_dir = './agora_'
elif math == 'sr2':
    output_dir = './sr2_'

    
def SaveData(entity, counter, prod):
    print("Saving data...")
    if counter % 50000 == 0: 
        number = str(int(counter / 50000))
    else:
        number = str(int(counter / 50000)+1)
    pickle.dump({'data' : prod}, open(output_dir + entity + "summaries_" + number + ".pkl", "wb"))
    prod = []
    return prod


def CreateProductSummaries():
    
    """
    Creates summaries for the product entities
    """
    
    counter = 0
    prod = []
    # ----- Pull all Products from the database
    products = session.query(Products).all()
    rev_by_product = session.query(func.sum(Reviews.price_euro)).filter(Reviews.price_euro < 5000).group_by(Reviews.product_id)
    revenuesorted = session.query(Reviews).order_by(desc(Reviews.timestamp))
    revenuesortedrev = session.query(Reviews).order_by(asc(Reviews.timestamp))
    
    for product in products:
        data = {}
        product_id = product.product_id
        images = session.query(Images.filename).filter(Images.product_id==product_id).all()

        if images!=None:
            if images[0][0] == None:
                data["Bilder"] = None
            else:
                images = [x[0]+".jpg" for x in images if x[0] != None]
                data["Bilder"] = images[0:np.minimum(len(images),5)]
        else:
            data["Bilder"] = None
        if math == 'agora':
            data["Marktplatz"] = 'Agora'
        elif math == 'sr2':   
            data["Marktplatz"] = 'Silkroad2'

        data["Produktname"] = product.name
        data["Kategorie"] = product.category
        priceterm = session.query(Prices.timestamp, Prices.price_euro).filter(Prices.product_id==product_id).order_by(desc(Prices.timestamp)).first()
        data["Preis"] = str(round(priceterm.price_euro,2)).replace(".",",") + " Euro am " + priceterm.timestamp.strftime("%d.%m.%y")
        data["Verkäufer"] = product.vendor
        revenue = rev_by_product.filter(Reviews.product_id==product_id).scalar()
        if revenue != None:
            data["Umsatz (gesamt)"] = str(round(revenue,2)).replace(".",",") + " Euro" 
        else:
            data["Umsatz (gesamt)"] = str(0) + " Euro"
        data["Versand aus"] = product.origin
        data["Versand nach"] = product.destination if product.destination != "null" else None
        data["Produkt aktiv"] = product.first_seen.strftime("%d.%m.%y") + " - " + product.last_seen.strftime("%d.%m.%y")
        data["Reviews"] = session.query(func.count(Reviews.product_id)).filter(Reviews.product_id==product_id).scalar()
        last_review = revenuesorted.filter(Reviews.product_id==product_id).first()
        first_review = revenuesortedrev.filter(Reviews.product_id==product_id).first()
        if last_review == None:
            data["Verkäufe von bis"] = None
        else:
            last_review = last_review.timestamp.strftime("%d.%m.%y")
            first_review = first_review.timestamp.strftime("%d.%m.%y")
            data["Verkäufe von bis"] =  first_review + " - " + last_review
        profile = session.query(Productprofiles.profile, Productprofiles.timestamp).filter(Productprofiles.product_id==product_id).first()
        if profile != None:
            data["Beschreibung"] = "(am " + profile.timestamp.strftime("%d.%m.%y") + ") " + profile.profile.replace("\\u201"," ").replace("\\n"," ").replace("\n","") 
        else:
            data["Beschreibung"] = None
        
        data["ID"] = product_id      

        prod.append(json.dumps(data))
        counter += 1
        print("Products: " + str(counter))

        if counter > 0 and counter % 50000 == 0:
            prod = SaveData("product_", counter, prod)

    prod = SaveData("product_", counter, prod)
            
             
def CreateVendorSummaries():
    
    """
    Creates summaries for the product entities
    """
    
    counter = 0
    prod = []
    # ----- Pull all Vendors from the database
    vendors = session.query(Vendors).all()
    rev_by_vendor = session.query(func.sum(Reviews.price_euro)).filter(Reviews.price_euro < 5000).group_by(Reviews.vendor_id)
    categories = session.query(Products.vendor_id, Products.category, func.count(Products.category)).group_by(Products.vendor_id, Products.category)
    
    for vendor in vendors:
        data = {}
        vendor_id = vendor.vendor_id
        if math == 'agora':
            data["Marktplatz"] = 'Agora'
        elif math == 'sr2':   
            data["Marktplatz"] = 'Silkroad2'
        data["Verkäufer"] = vendor.vendor_name
        data['Anzahl der Produkte'] = session.query(func.count(Products.name)).filter(Products.vendor_id==vendor_id).scalar()
        vendor_categories = categories.filter(Products.vendor_id==vendor_id)
        sorted_vc = sorted(vendor_categories, key=lambda tup: tup[2], reverse=True)
        vc = [(x[1], x[2]) for x in sorted_vc if x[1]!=None] 
        data["Produkte in Kategorien"] = [(x[0], x[1]) for x in vc][0:np.minimum(len(vc),8)]
        # No rating on SR2M
        data['Reviews'] = session.query(func.count(Reviews.vendor_id)).filter(Reviews.vendor_id==vendor_id).scalar()
        revenue = rev_by_vendor.filter(Reviews.vendor_id==vendor_id).scalar()
        if revenue != None:
            data['Umsatz (gesamt)'] = str(round(revenue,2)).replace(".",",") + " Euro" 
        else:
            data['Umsatz (gesamt)'] = str(0) + " Euro"
        origins = session.query(Products.origin).filter(Products.vendor_id==vendor_id).all()
        data['Produkte aus Deutschland'] = "Nein"
        for ori in list(origins):
            if 'Germany' in ori or 'GERMANY' in ori or 'German' in ori:
                data['Produkte aus Deutschland'] = "Ja"
                break
        # no verifications on SR2
        hsp = session.query(Products)  \
            .join(Reviews, Products.product_id==Reviews.product_id)    \
            .filter(Reviews.price_euro < 5000, Reviews.vendor_id == vendor_id)   \
            .group_by(Products.name, Reviews.product_id)   \
            .values(Products.name, func.sum(Reviews.price_euro))

        sorted_hsp = sorted(hsp, key=lambda tup: tup[1], reverse=True)
        hsp = [(x[0], x[1]) for x in sorted_hsp] 
        data["Produkte mit höchstem Umsatz"] = [(x[0], str(round(x[1],2)).replace(".",",") + " Euro") for x in hsp][0:np.minimum(len(hsp),8)]
        if math == 'sr2':        
            data["Verkäufer seit"] = vendor.vendorSince.strftime("%d.%m.%y")
        pgp_key = session.query(PGP_Keys.pgp_key).filter(PGP_Keys.vendor_id==vendor_id).order_by(desc(PGP_Keys.timestamp)).first()
        if math == 'agora':
             rating = session.query(Vendors.rating).filter(Vendors.vendor_id==vendor_id).first()
             if rating != None:
                 data['Bewertung'] = rating[0]
             else:
                 data['Bewertung'] = None
        profile = session.query(Vendorprofiles.profile, Vendorprofiles.timestamp).filter(Vendorprofiles.vendor_id==vendor_id).order_by(desc(Vendorprofiles.timestamp)).first()
        if profile != None:
            profilex = profile.profile
            if pgp_key[0] != None:
                try:
                    profilex = profilex[0:profilex.index(pgp_key[0][0:10])] 
                except ValueError:
                    try:
                        profilex = profilex[0:profilex.index("PGP keyVersion:")]
                    except ValueError:
                        pass
            if "vendor feedback" in profilex:
                profilex = profilex[:profilex.index("vendor feedback")].strip()
            data["Profil"] = "(am " + profile.timestamp.strftime("%d.%m.%y") + ") " + profilex.replace("\\u201"," ").replace("\\n"," ").replace("\n","")
        else:
            data["Profil"] = "" 
        if pgp_key != None:
            data["PGP-Schlüssel"] = pgp_key[0]
        else:
            data["PGP-Schlüssel"] = None
        data["ID"] = vendor_id

        prod.append(json.dumps(data))
        counter += 1
        print("Vendor: " + str(counter))

        if counter > 0 and counter % 50000 == 0:
            prod = SaveData("vendor_", counter, prod)

    prod = SaveData("vendor_", counter, prod)
    
    
def CreateCategorySummaries():
    
    """
    Creates summaries for the categories
    """
    
    counter = 0
    prod = []
    # ----- Pull all caetegories from the database
    categories = session.query(Products.category, Products.category_id).group_by(Products.category, Products.category_id)
    
    for category in categories:
        data = {}
        category_id = category.category_id
        if math == 'agora':
            data["Marktplatz"] = 'Agora'
        elif math == 'sr2':   
            data["Marktplatz"] = 'Silkroad2'
        data["Kategorie"] = category.category
        if data["Kategorie"] == None:
            continue
        data["Anzahl der Produkte"] = session.query(func.count(Products.name)).filter(Products.category_id==category_id).scalar()
        revenue = session.query(func.sum(Reviews.price_euro))  \
            .join(Products, Products.product_id==Reviews.product_id)    \
            .filter(Reviews.price_euro < 5000, Products.category_id == category_id).scalar()
        if revenue != None:
            data["Umsatz (gesamt)"] = str(round(revenue,2)).replace(".",",") + " Euro"
        else:
            data["Umsatz (gesamt)"] = 0          
        if math == 'agora':
            data["Betrachteter Zeitraum"] = "Januar 2014 - Juli 2015"
        elif math == 'sr2':   
            data["Betrachteter Zeitraum"] = "Dezember 2013 - November 2014"
        

        prod.append(json.dumps(data))
        counter += 1
        print("Category: " + str(counter))

        if counter > 0 and counter % 50000 == 0:
            prod = SaveData("category_", counter, prod)

    prod = SaveData("category_", counter, prod)

def CreateMarketplaceSummaries():
    
    """
    Creates summaries for the marketplace
    """
    
    prod = []
    data = {}
    if math == 'agora':
        data["Bild"] = "agora.png"
        data["Marktplatz"] = "Agora"
        data["URL"] = "agorahooawayyfoe.onion"
        data["Währung"] = "Bitcoin"
        data["Online"] = "Februar 2013 - August 2015"        
        data["Umsatz (gesamt)"] = "67 Millionen Euro"
        data["Betrachteter Zeitraum"] = "Januar 2014 - Juli 2015" 
        categories = session.query(Products.category).group_by(Products.category).all()
        data["Kategorien"] = [x[0] for x in categories if x[0] is not None]
        print(data)          
    elif math == 'sr2':   
        data["Bild"] = "silkroad2.png"
        data["Marktplatz"] = "Silkroad 2"
        data["URL"] = "silkroad6ownowfk.onion"
        data["Währung"] = "Bitcoin"
        data["Online"] = "November 2013 - November 2014"
        data["Umsatz (gesamt)"] = "70 Millionen Euro"
        data["Betrachteter Zeitraum"] = "Dezember 2013 - November 2014"
        categories = session.query(Products.category).group_by(Products.category).all()
        data["Kategorien"] = [x[0] for x in categories if x[0] is not None]
    prod.append(json.dumps(data))
    prod = SaveData("marketplace_", 1, prod)

            
def main():
    CreateProductSummaries()
    CreateVendorSummaries()
    CreateCategorySummaries()
    CreateMarketplaceSummaries()   
    

if __name__=='__main__':
    start_time = time.time()
    try:
        main()
    except Exception:
        print("An Error occured...")
        traceback.print_exc()  
    print("--- %s seconds ---" % (time.time() - start_time))