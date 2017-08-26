from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

db_string = 'postgres://postgres:asdf@localhost/ProductTest'  

db = create_engine(db_string)  
base = declarative_base()

class Marketplaces(base):
    __tablename__ = 'marketplaces'
    marketplace_id = Column(String(3), primary_key=True)
    marketplace_description = Column(String)
    
class Products(base): 
    __tablename__ = 'products'
    product_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    category_id = Column(Integer, ForeignKey("categories.category_id"))
    name = Column(String, nullable=False)
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    vendor = Column(String)
    category = Column(String)
    origin = Column(String)
    destination = Column(String)
    last_seen = Column(Integer)
    first_seen = Column(Integer)
    date = Column(Integer, nullable=False)

class Vendors(base):
    __tablename__ = 'vendors'
    vendor_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    vendor_name = Column(String)
    vendorSince = Column(Integer)

class Prices(base): 
    __tablename__ = 'prices'
    price_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.product_id"), nullable=False)
    marketplace_id = Column(String(3), ForeignKey('marketplaces.marketplace_id'))
    timestamp = Column(Integer, nullable=False)
    price  = Column(Float, nullable=False)
    exchange_rate = Column(Float, nullable=False)
    price_dollar = Column(Float, nullable=False)

class Reviews(base): 
    __tablename__ = 'reviews'
    sale_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.product_id"), nullable=False)
    marketplace_id = Column(String(3), ForeignKey("marketplaces.marketplace_id"))
    vendor_id = Column(Integer, nullable=False)
    category_id = Column(Integer, ForeignKey("categories.category_id"))
    timestamp = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    evaluation = Column(String)
    reviewtext = Column(String)
    
class Categories(base): 
    __tablename__ = 'categories'
    category_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey("marketplaces.marketplace_id"))
    category_1 = Column(String)
    category_2 = Column(String)
    category_3 = Column(String)
    category_4 = Column(String)
    category_5 = Column(String)
    category_6 = Column(String)
    
class PGP_Keys(base): 
    __tablename__ = 'pgp_keys'
    pgp_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey("marketplaces.marketplace_id"))
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    timestamp = Column(Integer)
    pgp_key = Column(String)   
    
class Profiles(base): 
    __tablename__ = 'profiles'
    profile_id = Column(Integer, primary_key=True)
    marketplace_id = Column(String(3), ForeignKey("marketplaces.marketplace_id"))
    vendor_id = Column(Integer, ForeignKey('vendors.vendor_id'))
    timestamp = Column(Integer)
    profile = Column(String) 

Session = sessionmaker(db)  
session = Session()
base.metadata.create_all(db)