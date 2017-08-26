def CheckForNovelty(pagelist, fdate):
    for page in pagelist:
        try:
            with open(page, encoding='utf8') as fname:
                soup = BeautifulSoup(fname, "lxml")
                if soup.find_all(id = 'content') and soup.find_all('h2'):
                    if len(soup.find_all('span', attrs={'class','next'})) > 1:
                        pass
                    else:
                        itemfeedbcand = soup.find_all('h3')
                        for cand in itemfeedbcand:
                            if 'item feedback' in cand:
                                thistable = cand.find_next('table')
                                reviews = len(thistable.find_all('tr'))-1
                                break
                        
                        # - optional - perform fast check via PullLastCrawlDate
                        
                        if reviews > 4:
                            # all relevant information is on this page
                            for i in range(1, 6): 
                                evaluation = thistable.find_all('tr')[int(reviews)+1-i].find_all('td')[0].get_text().replace(' of 5','')
                                if '★' in evaluation:
                                    evaluation = "none"
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
                                # QUery the database based on evaluation date (plus minus1), reviewtext, vendor and marketplace
                                # if exists pull product_id put it somewhere
        
                        else:
                            # collect them and jump one back 
                            # do the same
                            
                            
                        #perform analysis og product id array
                        
                        if conclusive:
                            return True
                        else:
                            return False
                        
                else:
                    continue
        except(Error):
            continue



def PullLastCrawlDate(fdate):
    crawls = sorted(os.listdir(data_source))
    for i in range(len(crawls)):
        if fdate == crawls[i]:
            return crawls[i-1]
    return None
        

def TransformTimestamp(fdate):
    if fdate.day > 9: 
        crawlDay = fdate.day
    else: crawlDay = "0" + str(fdate.day)
    if fdate.month < 9: 
        crawlMonth = "0" + str(fdate.month)
    else:
        crawlMonth = fdate.month
    currentTimestamp = str(fdate.year) + str(crawlMonth) + str(crawlDay)
    currentTimestamp = int(currentTimestamp)
    return currentTimestamp