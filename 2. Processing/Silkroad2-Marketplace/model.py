"""
This file establishes the schema for the to be filled database for the data from Silkroad2-Marketplace
"""

from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Float, ForeignKey, and_, asc, desc, DateTime
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

# For example postgres://user:password@localhost/SR2M
db_string = ""
db = create_engine(db_string)  

base = declarative_base()

class Marketplaces(base):
    __tablename__ = 'marketplaces'
    marketplace_id = Column(String(3), primary_key=True)
    marketplace_description = Column(String, nullable=False)
    
class Products(base): 
    __tablename__ = 'products'
    product_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    category_id = Column(Integer, ForeignKey('categories.category_id'))
    category = Column(String)
    name = Column(String, nullable=False)
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    vendor = Column(String)
    origin = Column(String)
    destination = Column(String)
    last_seen = Column(DateTime)
    first_seen = Column(DateTime)
    date = Column(DateTime, nullable=False)
    source = Column(String, nullable=False)
    srproductid = Column(Integer, nullable=False)

class Vendors(base):
    __tablename__ = 'vendors'
    vendor_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    vendor_name = Column(String, nullable=False)
    vendorSince = Column(DateTime)

class Prices(base): 
    __tablename__ = 'prices'
    price_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.product_id'))
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    timestamp = Column(DateTime, nullable=False)
    price  = Column(Float, nullable=False)
    source = Column(String, nullable=False)
    srproductid = Column(Integer, nullable=False)
    
class Reviews(base): 
    __tablename__ = 'reviews'
    sale_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.product_id'))
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    timestamp = Column(DateTime, nullable=False)
    price = Column(Float, nullable=False)
    evaluation = Column(String)
    reviewtext = Column(String)
    source = Column(String, nullable=False)
    srproductid = Column(Integer, nullable=False)  
    price_dollar = Column(Float)  
    
class Categories(base): 
    __tablename__ = 'categories'
    category_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    category_1 = Column(String, nullable=False)
    category_2 = Column(String)
    category_3 = Column(String)
    category_4 = Column(String)
    category_5 = Column(String)
    category_6 = Column(String)
    
class PGP_Keys(base): 
    __tablename__ = 'pgp_keys'
    pgp_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    timestamp = Column(DateTime)
    pgp_key = Column(String)
    source = Column(String, nullable=False)
    
class Vendorprofiles(base): 
    __tablename__ = 'vendorprofiles'
    vendorprofile_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    timestamp = Column(DateTime)
    profile = Column(String) 
    source = Column(String, nullable=False)
    
class Productprofiles(base): 
    __tablename__ = 'productprofiles'
    productprofile_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    product_id = Column(Integer, ForeignKey('products.product_id'))
    timestamp = Column(DateTime)
    profile = Column(String) 
    source = Column(String, nullable=False)
    srproductid = Column(Integer, nullable=False)
            
class Images(base): 
    __tablename__ = 'images'
    image_id =  Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    product_id = Column(Integer, ForeignKey('products.product_id'))
    timestamp = Column(DateTime)
    filename = Column(String) 
    source = Column(String, nullable=False)
    srproductid = Column(Integer, nullable=False)
    
Session = sessionmaker(db)  
session = Session()
base.metadata.create_all(db)
