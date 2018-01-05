# -*- coding: utf-8 -*-

"""
---------------------------------------------------------------------
Description
---------------------------------------------------------------------
This script will be used to translate information from the Dogeroad-Forum
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
import time 
import dateutil 
import os
import logging
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

                    
def GetandSaveData(timeframe, category, cand, fpath):
    """
    Nested Function to save lines from the statistical infromation pages 
    """

    users = list(cand.find_next('dl', attrs={'class', 'stats'}).find_all('dt'))
    labels = list(cand.find_next('dl', attrs={'class', 'stats'}).find_all('dd'))
    try:
        user_1 = users[0].get_text().strip()
        label_1 = labels[0].get_text().strip()
    except IndexError:  
        user_1 = None
        label_1 = None   
    try:        
        user_2 = users[1].get_text().strip()
        label_2 = labels[1].get_text().strip()
    except IndexError:
        user_2 = None
        label_2 = None 
    try:        
        user_3 = users[2].get_text().strip()
        label_3 = labels[2].get_text().strip()
    except IndexError: 
        user_3 = None
        label_3 = None 
    try:        
        user_4 = users[3].get_text().strip()
        label_4 = labels[3].get_text().strip()
    except IndexError:  
        user_4 = None
        label_4 = None         
    try:        
        user_5 = users[4].get_text().strip()
        label_5 = labels[4].get_text().strip()
    except IndexError: 
        user_5 = None
        label_5 = None          
    try:        
        user_6 = users[5].get_text().strip()
        label_6 = labels[5].get_text().strip()
    except IndexError: 
        user_6 = None
        label_6 = None          
    try:        
        user_7 = users[6].get_text().strip()
        label_7 = labels[6].get_text().strip()
    except IndexError: 
        user_7 = None
        label_7 = None          
    try:        
        user_8 = users[7].get_text().strip()
        label_8 = labels[7].get_text().strip()
    except IndexError: 
        user_8 = None
        label_8 = None          
    try:        
        user_9 = users[8].get_text().strip()
        label_9 = labels[8].get_text().strip()
    except IndexError:  
        user_9 = None
        label_9 = None         
    try:        
        user_10 = users[9].get_text().strip()
        label_10 = labels[9].get_text().strip()
    except IndexError:          
        user_10 = None
        label_10 = None 
    stats = session.query(DORForumMonthlyStats2).filter(and_(DORForumMonthlyStats2.forum_id=='DORF', DORForumMonthlyStats2.timeframe==timeframe, DORForumMonthlyStats2.category==category)).first()
    if not stats:
        newMonthlyStats = DORForumMonthlyStats2(forum_id='DORF', timeframe=timeframe, category=category, user_1=user_1, label_1=label_1, user_2=user_2, label_2=label_2, user_3=user_3, label_3=label_3, user_4=user_4, label_4=label_4, user_5=user_5, label_5=label_5, user_6=user_6, label_6=label_6, user_7=user_7, label_7=label_7, user_8=user_8, label_8=label_8, user_9=user_9, label_9=label_9, user_10=user_10, label_10=label_10, source=fpath)
        session.add(newMonthlyStats)
        session.commit()        

         
def files(path):
    """
    Filters files from a directory
    """
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file    

def CollectTopicInformation(fpath, datestr, currentTimestamp):
    """
    Takes a collection of file paths for a topic and extracts required information, like threadname, replies, repliers etc.
    """
    maxreply = None
    logger.debug('processing: {}'.format(fpath))
    with open(fpath, encoding='utf8') as fname:
        soup = BeautifulSoup(fname, 'lxml')
         
    if soup.find_all('div', attrs={'class', 'post_wrapper'}):

        for post in soup.find_all('div', attrs={'class', 'post_wrapper'}):    
            headElement = post.find_all('div', attrs={'class', 'smalltext'})[0].find_next('strong')
            replynumber = headElement.get_text()
            threadbool = False            
            if 'Reply' in replynumber:
                # ----- It's a reply, not the main post
                # ----- The replynumber is how we determine if the reply was already saved to the database
                replynumber = replynumber[replynumber.index("#")+1: replynumber.index(" ", replynumber.index("#"))]
                replynumber = int(replynumber)
                if maxreply:
                    if replynumber <= maxreply:
                        continue
            else:
                # ----- It's a thread, not a reply
                threadbool = True
                
            timestamp = headElement.next_sibling.replace("»","").strip()
            if len(timestamp)>0:
                timestamp = dateutil.parser.parse(timestamp)
            else: 
                if 'Yesterday' in post.find_all('div', attrs={'class', 'smalltext'})[0].get_text():
                    timestamp = datestr + ", " + list(post.find_all('div', attrs={'class', 'smalltext'})[0].contents)[-1].replace("»","").replace("at","").strip()
                    timestamp = dateutil.parser.parse(timestamp)
                    timestamp = timestamp.replace(day=timestamp.day-1)
                elif 'Today' in post.find_all('div', attrs={'class', 'smalltext'})[0].get_text():
                    timestamp = datestr + ", " + list(post.find_all('div', attrs={'class', 'smalltext'})[0].contents)[-1].replace("»","").replace("at","").strip()
                    timestamp = dateutil.parser.parse(timestamp)
                else:
                    raise NameError("timestamps?")  
                    
            # ----- Determine the username of the user that started the thread or left the reply                
            username = post.find_next('h4').get_text().strip() 
            # ----- Check if the user is already in the database
            users = session.query(Users).filter(and_(Users.forum_id=='DORF', Users.name==username)).first()
            if users:
                user_id = users.user_id
            else:
                # ----- if the user is not there add user with their postnumber and karma
                try:
                    posts = post.find_all('li', attrs={'class', 'postcount'})[0].get_text().replace("Posts:","").strip()
                except:
                    posts = None
                newUser = Users(forum_id='DORF', name=username, posts=posts, last_seen=timestamp, timestamp=timestamp, date_registered=None, source=fpath)                                                                 
                session.add(newUser)
                session.flush()   
                session.refresh(newUser)
                session.commit()
                user_id = newUser.user_id
                
            # ----- Find the message (thread or reply)            
            message = post.find('div', attrs={'class', 'post'}).find('div', attrs={'class', 'inner'})
            try:
                # ----- Annotate beginning and end of a quote
                if message.find('div', attrs={'class', 'quoteheader'}).find("a"):
                    core = message.find("a").get_text()
                    core += " BEGINQUOTE "
                    core += message.find("blockquote").get_text()
                    core += " ENDQUOTE "
                    for i in range(2,len(list(message.children))):
                        if str(type(list(message.children)[i]))=="<class 'bs4.element.NavigableString'>":
                            core += list(message.children)[i]
                    message = core
                else:
                    raise NameError
            except:
                message = message.get_text()

            if threadbool == True:
                # ---- if it is a thread, not a reply, check if it is in the database
                threadid = soup.find_all('div', attrs={'class', 'post_wrapper'})[0].find('div', attrs={'class', 'post'}).find('div', attrs={'class', 'inner'})["id"]
                thread = session.query(Threads).filter(and_(Threads.forum_id=='DORF', Threads.threadid==threadid)).first()
                if not thread:
                    # ----- Add the thread to the database
                    title = soup.find_all('h3', attrs={'class', 'catbg'})[-1].children
                    title = list(title)[-1].strip()
                    
                    # ----- Add the board it was published on
                    boards = soup.find_all('div', attrs={'class', 'navigate_section'})[0].find_all('a')
                    for candidate in boards:
                        if 'board' in candidate['href']:
                            candidatehref = candidate['href']
                            boardnumber = candidatehref[candidatehref.index("board")+6:]
                            boardname = list(candidate.children)[0].get_text()
                            break

                    # ----- if the board itself is not saved yet, save it                  
                    boards = session.query(Boards).filter(and_(Boards.forum_id=='DORF', Boards.boardnumber==boardnumber)).first()
                    if not boards:
                        boards = Boards(forum_id='DORF', boardnumber=boardnumber, boardname=boardname, source=fpath)                                                          
                        session.add(boards)
                        session.flush()
                        session.refresh(boards)
                        session.commit()                    
                    board_id = boards.board_id
                    
                    thread = Threads(forum_id='DORF', user_id=user_id, board_id=board_id, message=message, title=title, threadid=threadid, timestamp=timestamp, source=fpath)                                                          
                    session.add(thread)
                    session.flush()
                    session.refresh(thread)
                    session.commit()
                thread_id = thread.thread_id
                if maxreply == None:
                    try:
                        maxreply = session.query(Replies).filter(and_(Replies.forum_id=='DORF', Replies.thread_id==thread_id)).order_by(desc(Replies.replynumber)).first()
                        maxreply = maxreply.replynumber
                    except:
                        pass
            else:
                # ----- Add the reply to the database
                newReply = Replies(forum_id='DORF', user_id=user_id, message=message, timestamp=timestamp, thread_id = thread_id, replynumber = replynumber, source=fpath)                                                          
                session.add(newReply)
                session.commit()  
                
                
def CleanUpTimestamp(timestamp, fdate):
    """
    Clean up expression function
    """
    
    if 'Yesterday' in timestamp:
        timestamp = fdate + ", " + timestamp.replace("»","").replace("Yesterday at","").strip()
        timestamp = dateutil.parser.parse(timestamp)
        timestamp = timestamp.replace(day=timestamp.day-1)
    elif 'Today' in timestamp:
        timestamp = fdate + ", " + timestamp.replace("»","").replace("Today at","").strip()
        timestamp = dateutil.parser.parse(timestamp)
    elif 'N/A' in timestamp:
        timestamp = None
    else:
        timestamp = dateutil.parser.parse(timestamp) 
    return timestamp 

               
def CollectUserInformation(file_path, file_date, currentTimestamp):
    
    """
    Takes a file path for a user profile page and extracts required information, like name, profile, etc.
    """
    
    logger.debug('processing: {}'.format(file_path))
    with open(file_path, encoding='utf8') as fname:
        soup = BeautifulSoup(fname, 'lxml')
        # ----- Find the username
        try:
            username = soup.find_all('div', attrs={'class', 'username'})[0].find_next("h4").contents[0]
        except:
            return
        
        # ----- Find level, posts, karma, pgp-key and date-registered
        level = soup.find_all('div', attrs={'class', 'username'})[0].find_next("h4").contents[1].get_text()
        contents = soup.find_all('div', attrs={'class', 'content'})[1].find_all("dt")
        pgp_key = None
        email = None
        personaltext = None
        location = None

        for child in contents:
            if "Posts:" in child.get_text():
                posts = child.find_next("dd").get_text()
            if "Age:" in child.get_text():
                age = child.find_next("dd").get_text()
            if "PGP Public Key:" in child.get_text():          
                pgp_key = child.find_next("dd").get_text()
            if "Date Registered:" in child.get_text():
                dateRegistered = child.find_next("dd").get_text().replace("»","")
                dateRegistered = CleanUpTimestamp(dateRegistered, file_date)
            if "Last Active:" in child.get_text():
                lastActive = child.find_next("dd").get_text().replace("»","")
                lastActive = CleanUpTimestamp(lastActive, file_date)               
            if "Email:" in child.get_text():
                email = child.find_next("dd").get_text().replace("»","")
            if "Personal Text:" in child.get_text():
                personaltext = child.find_next("dd").get_text().replace("»","")
            if "Location:" in child.get_text():
                location = child.find_next("dd").get_text().replace("»","")                 

        # ----- Check if the user is in the database
        profile = session.query(Users).filter(and_(Users.forum_id=='DORF', Users.name==username)).first()  
        if profile == None:
            # ----- If the user is not in the database, add the user
            newUser = Users(forum_id='DORF', name=username, posts=posts, email=email, age=age, date_registered=dateRegistered, last_seen=lastActive, timestamp=currentTimestamp, source=file_path)                                                          
            session.add(newUser)
            session.flush()   
            session.refresh(newUser)
            # ----- Determine the user_id for further processing
            user_id = newUser.user_id
            
        else:
            # ----- If the user is there, update the information for that user
            user_id = profile.user_id
            profile.posts = posts
            profile.last_seen = lastActive
            if email != None:
                profile.email = email
            profile.age = age
            profile.currenttimestamp = currentTimestamp
            profile.source = file_path
            
            
        # ----- Check if the pgp-key for that user is already saved             
        if pgp_key!=None:
            isThere = False
            pgp = session.query(PGPKeys).filter(and_(PGPKeys.forum_id=='DORF', PGPKeys.user_id==user_id, PGPKeys.pgp_key==pgp_key)).all()  
            for key in pgp:
                if pgp_key == key.pgp_key:
                    isThere = True
                    break
            if isThere == False:
                newPGP = PGPKeys(forum_id='DORF', user_id=user_id, pgp_key=pgp_key, timestamp=currentTimestamp, source=file_path)  
                session.add(newPGP)
                session.commit()   
                
        # ----- Check if the personalmessage for that user is already saved   
        if personaltext!=None:
            isThere = False
            text = session.query(Personalmessage).filter(and_(Personalmessage.forum_id=='DORF', Personalmessage.user_id==user_id, Personalmessage.personaltext==personaltext)).all()  
            for te in text:
                if personaltext == te.personaltext:
                    isThere = True
                    break
            if isThere == False:
                personaltext = Personalmessage(forum_id='DORF', user_id=user_id, personaltext=personaltext, timestamp=currentTimestamp, source=file_path)   
                session.add(personaltext)
                session.commit()
                
        # ----- Check if the location for that user is already saved  
        if location!=None:
            isThere = False
            locations = session.query(Locations).filter(and_(Locations.forum_id=='DORF', Locations.user_id==user_id, Locations.location==location)).all()  
            for loc in locations:
                if location == loc.location:
                    isThere = True
                    break
            if isThere == False:
                location = Locations(forum_id='DORF', user_id=user_id, location=location, timestamp=currentTimestamp, source=file_path)   
                session.add(location)
                session.commit()

        # ----- Save or update the level    
        lastLevel = session.query(Levels).filter(and_(Levels.forum_id=='DORF', Levels.user_id==user_id)).order_by(desc(Levels.timestamp)).first() 
        if lastLevel.level != level:
            newLevel = Levels(forum_id='DORF', user_id=user_id, level=level, timestamp=currentTimestamp, source=file_path) 
            session.add(newLevel)
            session.commit()   
        else:
            lastLevel.timestamp = currentTimestamp
                           
                
def CollectStatisticalInformation(file_path, currentTimestamp):  
    """
    Collect the data from the statistical information pages of the forum
    """
    logger.debug('processing: {}'.format(file_path))
    with open(file_path, encoding='utf8') as fname:
        soup = BeautifulSoup(fname, 'lxml')
        history = soup.find_all('h3')[1]
        if not "Forum History" in history.get_text():
            raise NameError("h3 Tag for statistic history not found...")
        else:
            for row in history.find_next('table').find_next('tbody').find_all('tr', attrs={'class', 'windowbg2'}):
                if len(row.find_all("th"))== 5: 
                    timeframe = row.find_all("th")[0].get_text().strip()
                    boards = session.query(DORForumMonthlyStats1).filter(and_(DORForumMonthlyStats1.forum_id=='DORF', DORForumMonthlyStats1.timeframe==timeframe)).first()
                    if not boards:
                        newTopics = row.find_all("th")[1].get_text().strip()
                        newPosts = row.find_all("th")[2].get_text().strip()
                        newMembers = row.find_all("th")[3].get_text().strip()
                        mostOnline = row.find_all("th")[4].get_text().strip()
                        newMonthlyStats = DORForumMonthlyStats1(forum_id='DORF', timeframe=timeframe, newTopics=newTopics, newPosts=newPosts, newMembers = newMembers, mostOnline = mostOnline, source=file_path)
                        session.add(newMonthlyStats)
                        session.commit() 

        timestamp = file_path[file_path.index("=", file_path.index("collapse"))+1:] 
        timestamp = timestamp[4:]+"-"+timestamp[0:4]
        bestcand = soup.find_all('div', attrs={'class', 'title_bar'})
        for cand in bestcand:
            if cand.get_text().strip() == 'Top 10 Posters':
                GetandSaveData(timestamp, 'Top 10 Posters', cand, file_path)                
            elif cand.get_text().strip() == 'Top 10 Boards':
                GetandSaveData(timestamp, 'Top 10 Boards', cand, file_path)  
            elif cand.get_text().strip() == 'Top 10 Topics (by Replies)':
                GetandSaveData(timestamp, 'Top 10 Topics (by Replies)', cand, file_path) 
            elif cand.get_text().strip() == 'Top 10 Topics (by Views)':
                GetandSaveData(timestamp, 'Top 10 Topics (by Views)', cand, file_path)  
            elif cand.get_text().strip() == 'Top Topic Starters':
                GetandSaveData(timestamp, 'Top Topic Starters', cand, file_path)  
            elif cand.get_text().strip() == 'Most Time Online':
                GetandSaveData(timestamp, 'Most Time Online', cand, file_path)
        
def main():

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
        file_path = os.path.join(data_source, datestr)
        currentTimestamp = dateutil.parser.parse(datestr)
        if os.path.isdir(file_path):
            contents = sorted(files(file_path))
            i = 0
            while i < len(contents):
                fpath = os.path.join(file_path, contents[i])
                if 'cur_topic_id' in fpath:
                    continue
                if "index.php_topic" in fpath:
                    pages = []
                    # ----- Find the leading page of the collection
                    leadingpage = fpath[0:fpath.index(".", fpath.index("topic")+5)]
                    pages.append(fpath)
                    for j in range(1, len(contents)-i):
                        fpath = os.path.join(file_path, contents[i+j])
                        try:
                             # ----- If the core of the pagename matches the leading page, add it to the collection
                            if fpath[0:fpath.index(".", fpath.index("topic")+5)] == leadingpage:
                                pages.append(fpath)
                            else:
                                raise NameError
                        except:
                            break
                    # ----- The collection of pages belonging to one topic is ready
                    i += len(pages)-1
                    for page in pages:
                        # ----- If there is a page with 'all' in the collection, take this page, since it does contain all the information, discard the rest
                        if 'all' in page:
                            pages = []
                            pages.append(page)
                            
                            try:
                                CollectTopicInformation(pages, datestr, currentTimestamp) 
                                break
                            except:
                                logger.exception("Error occured in Topic Information: {}".format(fpath))
                                logger.exception(traceback.print_exc())
              
                    try:
                        # Collect information from the entire collection (in case an 'all' page was not found)
                        CollectTopicInformation(pages, datestr, currentTimestamp)
                    except:
                        logger.exception("Error occured in Topic Information: {}".format(fpath))
                        logger.exception(traceback.print_exc())

                if "index.php_action=profile" in fpath:
                    # ----- Collect User Information
                    try:
                        CollectUserInformation(fpath, datestr, currentTimestamp)
                    except:
                        logger.exception("Error occured in User Information: {}".format(fpath))
                        logger.exception(traceback.print_exc())
                if "index.php_action=stats;collapse=" in fpath:
                    # ----- Collect Statistical Information
                    try:
                        CollectStatisticalInformation(fpath, currentTimestamp)
                    except:
                        logger.exception("Error occured in Statistical Information: {}".format(fpath))
                        logger.exception(traceback.print_exc())
                i += 1

if __name__=='__main__':
    start_time = time.time()
    try:
        main()
    except Exception:
        print("An Error occured...")
        traceback.print_exc()
    print("--- %s seconds ---" % (time.time() - start_time))