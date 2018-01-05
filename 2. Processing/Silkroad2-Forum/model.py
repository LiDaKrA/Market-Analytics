"""
This file establishes the schema for the to be filled database for the data from Silkroad2-Forum
"""
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, VARCHAR, and_, desc, asc
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

# For example postgres://user:password@localhost/SR2F
db_string = ""  
db = create_engine(db_string)  

base = declarative_base()

class Forums(base):
    __tablename__ = 'forums'
    forum_id = Column(String(4), primary_key=True)
    forum_description = Column(String, nullable=False)
    
class SR2ForumMonthlyStats1(base):
    __tablename__ = 'sr2monthlystats1'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    stats1_id = Column(Integer, primary_key=True)
    timeframe = Column(String, nullable=False)   
    newTopics = Column(Integer, nullable=False)  
    newPosts = Column(Integer, nullable=False)   
    newMembers = Column(Integer, nullable=False)    
    mostOnline = Column(Integer, nullable=False)    
    pageViews = Column(Integer, nullable=False)  
    source = Column(String, nullable=False)

class SR2ForumMonthlyStats2(base):
    __tablename__ = 'sr2monthlystats2'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    stats1_id = Column(Integer, primary_key=True)
    timeframe = Column(String, nullable=False) 
    category = Column(String, nullable=False)  
    user_1 = Column(String, nullable=False)  
    label_1 = Column(VARCHAR, nullable=False)
    user_2 = Column(String, nullable=False) 
    label_2 = Column(VARCHAR, nullable=False)
    user_3 = Column(String, nullable=False) 
    label_3 = Column(VARCHAR, nullable=False)
    user_4 = Column(String, nullable=False) 
    label_4 = Column(VARCHAR, nullable=False)
    user_5 = Column(String, nullable=False) 
    label_5 = Column(VARCHAR, nullable=False)
    user_6 = Column(String, nullable=False) 
    label_6 = Column(VARCHAR, nullable=False)
    user_7 = Column(String, nullable=False) 
    label_7 = Column(VARCHAR, nullable=False)
    user_8 = Column(String, nullable=False) 
    label_8 = Column(VARCHAR, nullable=False)
    user_9 = Column(String, nullable=False) 
    label_9 = Column(VARCHAR, nullable=False)
    user_10 = Column(String, nullable=False) 
    label_10 = Column(VARCHAR, nullable=False)
    source = Column(String, nullable=False)
    
class Users(base):
    __tablename__ = 'users'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    posts = Column(String) 
    karma = Column(String)
    date_registered = Column(DateTime)
    last_seen = Column(DateTime)
    timestamp = Column(String)
    source = Column(String, nullable=False)
    
class Signatures(base):
    __tablename__ = 'signatures'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(Integer, ForeignKey('users.user_id')) 
    signature_id = Column(Integer, primary_key=True)
    signature = Column(String)
    timestamp = Column(String)     
    source = Column(String, nullable=False)
    
class PGPKeys(base):
    __tablename__ = 'pgpkeys'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(Integer, ForeignKey('users.user_id')) 
    pgp_id = Column(Integer, primary_key=True)
    pgp_key = Column(String, nullable=False)
    timestamp = Column(String)       
    source = Column(String, nullable=False)
    
class Threads(base): 
    __tablename__ = 'threads'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(Integer, ForeignKey('users.user_id')) 
    thread_id = Column(Integer, primary_key=True)
    board_id = Column(Integer, ForeignKey('boards.board_id')) 
    title = Column(String) 
    message = Column(String)
    threadid = Column(String)
    timestamp = Column(DateTime)
    source = Column(String, nullable=False)
    
class Boards(base): 
    __tablename__ = 'boards'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    board_id = Column(Integer, primary_key=True)
    boardnumber = Column(String, nullable=False)
    boardname = Column(String, nullable=False) 
    source = Column(String, nullable=False)

class Replies(base):
    __tablename__ = 'replies'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(Integer, ForeignKey('users.user_id')) 
    reply_id = Column(Integer, primary_key=True)
    message = Column(String)
    timestamp = Column(DateTime)
    thread_id = Column(Integer)
    replynumber = Column(Integer)
    source = Column(String, nullable=False)

class Levels(base):
    __tablename__ = 'levels'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(Integer, ForeignKey('users.user_id')) 
    level_id = Column(Integer, primary_key=True)
    level = Column(String)
    timestamp = Column(String) 
    source = Column(String, nullable=False)
    
Session = sessionmaker(db)  
session = Session()
base.metadata.create_all(db)
