# This script will pull exchange rates and evaluate prices for reviews

import sys
# Specify working directory (if not done)
workingDirectory = "C://Project/SR2-Marketplace"
sys.path.append(workingDirectory)

import pandas as pd
import time
from sqlalchemy import create_engine  
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

# Specifications:
databaseReference = "postgres://postgres:asdf@localhost/ProductTest"
# Location of Bitcoin.csv
param_dir="C://USB/Bitcoin.csv"
# Manually add column "price_euro" in table "reviews" (if not there)
# ALTER TABLE reviews ADD COLUMN price_euro float

# Manually add column "price_euro" in table "reviews" (if not there)
# ALTER TABLE reviews ADD COLUMN price_euro float

db_string = databaseReference
db = create_engine(db_string)  
base = declarative_base()
Session = sessionmaker(db)  
session = Session()
base.metadata.create_all(db)

def FindExchangeRate(timestamp):
    source = pd.read_csv(param_dir, index_col='Date', sep=',', engine='python')
    #------- Forming datestring to look up the exchange rate
    if str(timestamp.month)[0]==0: 
        month = str(timestamp.month)[1]
    else:
        month = str(timestamp.month) 
    if str(timestamp.day)[0]==0: 
        day = str(timestamp.day)[1]
    else:
        day = str(timestamp.day) 
    modifiedDatestring = month + '/' + day + '/' + str(timestamp.year)

    try:
        exchange_rate = source.loc[modifiedDatestring,'Price'].replace(',','')
    except:
        day = str(timestamp.day + 1)
        modifiedDatestring = month + '/' + day  + '/' + str(timestamp.year)
        exchange_rate = source.loc[modifiedDatestring,'Price'].replace(',','')
    assert exchange_rate
    
    exchange_rate = float(exchange_rate)
    
    return exchange_rate


start_time = time.time()   
reviews = session.query(Reviews)

for review in reviews:
    price_euro = review.price * FindExchangeRate(review.timestamp)
    review.price_euro = price_euro
    #setattr(review, 'price_euro', price_euro)
    session.commit()
print("--- %s seconds ---" % (time.time() - start_time))    