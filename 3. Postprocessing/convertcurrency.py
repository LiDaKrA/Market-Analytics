"""
This script will pull exchange rates populate the 'price_euro' column in the reviews and prices tables
"""

import pandas as pd
import time
from model import *

# ----- Location of bit.csv
param_dir = "./bit.csv"

# Manually add column "price_euro" in table "reviews" (if not there) via
# ALTER TABLE reviews ADD COLUMN price_euro float

source = pd.read_csv(param_dir, index_col='Date', sep=',', engine='python')

def FindExchangeRate(timestamp):
    exchange_rate = source.loc[timestamp,'Value']
    assert exchange_rate
    exchange_rate = float(exchange_rate) 
    return exchange_rate

def main():
    
    # ----- Update the prices / price 'price_euro' column      
    prices = session.query(Prices).order_by(desc(Prices.timestamp))
    i=0
    oldTimestamp = None
    for pricex in prices:
        timestamp = pricex.timestamp.strftime('%d.%m.%Y')
    
        if timestamp != oldTimestamp:
            exchange_rate = FindExchangeRate(timestamp)
    
        price_euro = pricex.price * exchange_rate
        pricex.price_euro = price_euro
        print("Prices: ", i)
        i+=1
        oldTimestamp = timestamp
    session.commit() 
    
    # ----- Update the revenue / review 'price_euro' column    
    reviews = session.query(Reviews).order_by(desc(Reviews.timestamp))
    i=0
    oldTimestamp = None
    for reviewx in reviews:
        timestamp = reviewx.timestamp.strftime('%d.%m.%Y')
    
        if timestamp != oldTimestamp:
            exchange_rate = FindExchangeRate(timestamp)
    
        price_euro = reviewx.price * exchange_rate
        reviewx.price_euro = price_euro
        print("Reviews: ", i)
        i+=1
        oldTimestamp = timestamp
    session.commit()

if __name__=='__main__':
    start_time = time.time()
    try:
        main()
    except Exception:
        print("An Error occured...")
        traceback.print_exc()  
    print("--- %s seconds ---" % (time.time() - start_time))   