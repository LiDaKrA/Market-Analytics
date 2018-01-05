# -*- coding: utf-8 -*-

"""
---------------------------------------------------------------------
Description
---------------------------------------------------------------------
This script will be used to translate information from the Silkroad2-Forum
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
import dateutil.parser 
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

def CollectUserInformation(file_path, file_date, currentTimestamp):
    
    """
    Takes a file path for a user profile page and extracts required information, like name, profile, etc.
    """
    
    logger.debug('processing: {}'.format(file_path))
    with open(file_path, encoding='utf8') as file_name:
        soup = BeautifulSoup(file_name, 'lxml')
        # ----- Find the username
        try:
            username = soup.find_all('div', attrs={'class', 'username'})[0].find_next("h4").contents[0]
        except:
            return
        
        # ----- Find level, posts, karma, pgp-key and date-registered
        level = soup.find_all('div', attrs={'class', 'username'})[0].find_next("h4").contents[1].get_text()
        contents = soup.find_all('div', attrs={'class', 'content'})[1].find_all("dt")
        pgp_key = None
        for child in contents:
            if "Posts:" in child.get_text():
                posts = child.find_next("dd").get_text()
            if "Karma:" in child.get_text():
                karma = child.find_next("dd").get_text()
            if "PGP Public Key:" in child.get_text():          
                pgp_key = child.find_next("dd").get_text()
            if "Date Registered:" in child.get_text():
                dateRegistered = child.find_next("dd").get_text().replace("»","")
                if 'Yesterday' in dateRegistered:
                    dateRegistered = file_date + ", " + dateRegistered.replace("»","").replace("Yesterday at","").strip()
                    dateRegistered = dateutil.parser.parse(dateRegistered)
                    dateRegistered = dateRegistered.replace(day=dateRegistered.day-1)
                elif 'Today' in dateRegistered:
                    dateRegistered = file_date + ", " + dateRegistered.replace("»","").replace("Today at","").strip()
                    dateRegistered = dateutil.parser.parse(dateRegistered)
                elif 'N/A' in dateRegistered:
                    dateRegistered = None
                else:
                    dateRegistered = dateutil.parser.parse(dateRegistered)  

        # ----- Find signature            
        try:
            signature = soup.find_all('div', attrs={'class', 'signature'})[0].next_elements
            signature = list(signature)[3]
            signature = str(signature).strip()
        except:
            signature = None
                
        # ----- Check if the user is in the database
        profile = session.query(Users).filter(and_(Users.forum_id=='SR2F', Users.name==username)).first()  
        if profile == None:
            # ----- If the user is not in the database, add the user
            newUser = Users(forum_id='SR2F', name=username, posts=posts, karma=karma, date_registered=dateRegistered, last_seen=currentTimestamp, timestamp=currentTimestamp, source=file_path)                                                          
            session.add(newUser)
            session.flush()   
            session.refresh(newUser)
            session.commit()  
            # ----- Determine the user_id for further processing
            user_id = newUser.user_id
            
        else:
            # ----- If the user is there, update the information for that user
            user_id = profile.user_id
            profile.posts = posts
            profile.last_seen = currentTimestamp
            profile.karma = karma
            profile.currenttimestamp = currentTimestamp
            profile.source = file_path
            session.commit()  
            
            
        # ----- Check if the pgp-key for that user is already saved            
        if pgp_key!=None:
            isThere = None
            pgp = session.query(PGPKeys).filter(and_(PGPKeys.forum_id=='SR2F', PGPKeys.user_id==user_id, PGPKeys.pgp_key==pgp_key)).all()  
            for key in pgp:
                if pgp_key == key:
                    isThere = True
                    break
            # ----- If it is not there, add the pgp-key
            if isThere == None:
                newPGP = PGPKeys(forum_id='SR2F', user_id=user_id, pgp_key=pgp_key, timestamp=currentTimestamp, source=file_path)  
                session.add(newPGP)
                session.commit()  
                

        if signature != None:
            # ----- If not, add the signature
            newSignature = Signatures(forum_id='SR2F', user_id=user_id, signature=signature, timestamp=currentTimestamp, source=file_path) 
            session.add(newSignature) 
            session.commit()  
        
        # ----- Check if the signature has changed, if the signature has changed, add the new one, if it hasn't update the timestamp, if it is not there, add it        
        if signature != None:
            lastSignature = session.query(Signatures).filter(and_(Signatures.forum_id=='SR2F', Signatures.user_id==user_id)).order_by(desc(Signatures.timestamp)).first()  
            if lastSignature != None:
                if lastSignature.signature != signature:
                    newSignature = Signatures(forum_id='SR2F', user_id=user_id, signature=signature, timestamp=currentTimestamp, source=file_path) 
                    session.add(newSignature)
                    session.commit()  
                else:
                    lastSignature.timestamp = currentTimestamp  
            else:
                newSignature = Signatures(forum_id='SR2F', user_id=user_id, signature=signature, timestamp=currentTimestamp, source=file_path) 
                session.add(newSignature)
                session.commit()  
        # ----- Check if the level has changed, if it has changed, add the new one, if it hasn't update the timestamp, if it is not there, add it                
        lastLevel = session.query(Levels).filter(and_(Levels.forum_id=='SR2F', Levels.user_id==user_id)).order_by(desc(Levels.timestamp)).first() 
        if lastLevel.level != level:
            newLevel = Levels(forum_id='SR2F', user_id=user_id, level=level, timestamp=currentTimestamp, source=file_path) 
            session.add(newLevel)
            session.commit()  
        else:
            lastLevel.timestamp = currentTimestamp        
        
        

def CollectTopicInformation(pagelist, datestr, currentTimestamp):
    
    """
    Takes a collection of file paths for a topic and extracts required information, like threadname, replies, repliers etc.
    """
    
    fpath = pagelist[0]
    maxreply = None
    
    for page in pagelist:
        logger.debug('processing: {}'.format(page))
        with open(page, encoding='utf8') as file_name:
            soup = BeautifulSoup(file_name, 'lxml')
            
        for post in soup.find_all('div', attrs={'class', 'post_wrapper'}):    
            threadbool = False
            headElement = post.find_all('div', attrs={'class', 'smalltext'})[0].find_next('strong')
            replynumber = headElement.get_text()
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
            if len(timestamp) > 0:
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
                    raise NameError("timestamps. Todaz?")     
                    
            # ----- Determine the username of the user that started the thread or left the reply                    
            username = post.find_next('h4').get_text().strip()
            # ----- Check if the user is already in the database
            users = session.query(Users).filter(and_(Users.forum_id=='SR2F', Users.name==username)).first()
            if users:
                user_id = users.user_id
            else:
                # ----- if the user is not there add user with their postnumber and karma
                try:
                    posts = post.find_all('li', attrs={'class', 'postcount'})[0].get_text()
                except:
                    posts = None
                try:    
                    karma = post.find_all('li', attrs={'class', 'karma'})[0].get_text() 
                except:
                    karma = None
                newUser = Users(forum_id='SR2F', name=username, posts=posts, karma=karma, last_seen=timestamp, timestamp=timestamp, date_registered=None, source=fpath)                                                                 
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
                    raise NameError("There is a problem with a Quote. Check if the HTML is broken...")
            except:
                message = message.get_text()

            if threadbool == True:
                # ---- if it is a thread, not a reply, check if it is in the database
                threadid = soup.find_all('div', attrs={'class', 'post_wrapper'})[0].find('div', attrs={'class', 'post'}).find('div', attrs={'class', 'inner'})["id"]
                thread = session.query(Threads).filter(and_(Threads.forum_id=='SR2F', Threads.threadid==threadid)).first()
                if not thread:
                    # ----- Add the thread to the database
                    title = soup.find_all('h3', attrs={'class', 'catbg'})[-1].children
                    title = list(title)[-1].strip()
                    if not "Topic" in title:
                        title = soup.find_all('h3', attrs={'class', 'catbg'})[0].children
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
                    boards = session.query(Boards).filter(and_(Boards.forum_id=='SR2F', Boards.boardnumber==boardnumber)).first()
                    if not boards:
                        boards = Boards(forum_id='SR2F', boardnumber=boardnumber, boardname=boardname, source=fpath)                                                          
                        session.add(boards)
                        session.flush()
                        session.refresh(boards)
                        session.commit()                    
                    board_id = boards.board_id
                    
                    thread = Threads(forum_id='SR2F', user_id=user_id, board_id=board_id, message=message, title=title, threadid=threadid, timestamp=timestamp, source=fpath)                                                          
                    session.add(thread)
                    session.flush()
                    session.refresh(thread)
                    session.commit()
                thread_id = thread.thread_id
                if maxreply == None:
                    try:
                        maxreply = session.query(Replies).filter(and_(Replies.forum_id=='SR2F', Replies.thread_id==thread_id)).order_by(desc(Replies.replynumber)).first()
                        maxreply = maxreply.replynumber
                    except:
                        pass
            else:
                # ----- Add the reply to the database
                newReply = Replies(forum_id='SR2F', user_id=user_id, message=message, timestamp=timestamp, thread_id = thread_id, replynumber = replynumber, source=fpath)                                                          
                session.add(newReply)
                session.commit()   
    
            
def GetandSaveData(timeframe, category, cand, fpath):
    """
    Nested Function to save lines from the statistical infromation pages 
    """
    
    labels = list(cand.find_next('dl', attrs={'class', 'stats'}).find_all('dd'))
    user_1 = users[0].get_text().strip()
    label_1 = labels[0].get_text().strip()
    user_2 = users[1].get_text().strip()
    label_2 = labels[1].get_text().strip()
    user_3 = users[2].get_text().strip()
    label_3 = labels[2].get_text().strip()
    user_4 = users[3].get_text().strip()
    label_4 = labels[3].get_text().strip()
    user_5 = users[4].get_text().strip()
    label_5 = labels[4].get_text().strip()
    user_6 = users[5].get_text().strip()
    label_6 = labels[5].get_text().strip()
    user_7 = users[6].get_text().strip()
    label_7 = labels[6].get_text().strip()
    user_8 = users[7].get_text().strip()
    label_8 = labels[7].get_text().strip()
    user_9 = users[8].get_text().strip()
    label_9 = labels[8].get_text().strip()
    user_10 = users[9].get_text().strip()
    label_10 = labels[9].get_text().strip()
	stats = session.query(SR2ForumMonthlyStats2).filter(and_(SR2ForumMonthlyStats2.forum_id=='SR2F', SR2ForumMonthlyStats2.timeframe==timeframe, SR2ForumMonthlyStats2.category==category)).first()
    if not stats:
		newMonthlyStats = SR2ForumMonthlyStats2(forum_id='SR2F', timeframe=timeframe, category=category, user_1=user_1, label_1=label_1, user_2=user_2, label_2=label_2, user_3=user_3, label_3=label_3, user_4=user_4, label_4=label_4, user_5=user_5, label_5=label_5, user_6=user_6, label_6=label_6, user_7=user_7, label_7=label_7, user_8=user_8, label_8=label_8, user_9=user_9, label_9=label_9, user_10=user_10, label_10=label_10, source=fpath)
		session.add(newMonthlyStats)
		session.commit()      

         
def CollectStatisticalInformation(file_path, currentTimestamp):   
    """
    Collect the data from the statistical information pages of the forum
    """
    logger.debug('processing: {}'.format(file_path))
    with open(file_path, encoding='utf8') as fname:
        soup = BeautifulSoup(fname, 'lxml')
        history = soup.find_all('h3')[1]
        if not "Forum History" in history.get_text():
            raise NameError("H3 Tag for statistic history not found...")
        else:
            for row in history.find_next('table').find_next('tbody').find_all('tr', attrs={'class', 'windowbg2'}):
                if len(row.find_all("th"))== 6: 
                    timeframe = row.find_all("th")[0].get_text().strip()
                    boards = session.query(SR2ForumMonthlyStats1).filter(and_(SR2ForumMonthlyStats1.forum_id=='SR2F', SR2ForumMonthlyStats1.timeframe==timeframe)).first()
                    if not boards:
                        newTopics = row.find_all("th")[1].get_text().strip()
                        newPosts = row.find_all("th")[2].get_text().strip()
                        newMembers = row.find_all("th")[3].get_text().strip()
                        mostOnline = row.find_all("th")[4].get_text().strip()
                        pageViews = row.find_all("th")[5].get_text().strip()
                        newMonthlyStats = SR2ForumMonthlyStats1(forum_id='SR2F', timeframe=timeframe, newTopics=newTopics, newPosts=newPosts, newMembers = newMembers, mostOnline = mostOnline, pageViews = pageViews, source=file_path)
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

def files(path):
    """
    Filters files from a directory
    """
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file    
        
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
                logger.debug('processing: {}'.format(file_path))                
                if "index.php?topic" in fpath:
                    pages = []
                    try:
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
                    except:
                       logger.exception("Page omitted: {}".format(fpath))
                    try:
                        # Collect information from the entire collection (in case an 'all' page was not found)
                        CollectTopicInformation(pages, datestr, currentTimestamp) 
                    except:
                        logger.exception("Error occured in Topic Information: {}".format(fpath))
                        logger.exception(traceback.print_exc())
                if "index.php?action=profile" in fpath:
                    # ----- Collect User Information
                    try:
                        CollectUserInformation(fpath, datestr, currentTimestamp)
                    except:
                        logger.exception("Error occured in User Information: {}".format(fpath))
                        logger.exception(traceback.print_exc())
                if "index.php?action=stats;collapse=" in fpath:
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
    except:
        print("An Error occured...")
        traceback.print_exc()  
    print("--- %s seconds ---" % (time.time() - start_time))