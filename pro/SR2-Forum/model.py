from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Float, ForeignKey, and_, asc, desc
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

db_string = "postgres://postgres:asdf@localhost/ProductTest"  
db = create_engine(db_string)  

base = declarative_base()

class Forums(base):
    __tablename__ = 'forums'
    forum_id = Column(String(4), primary_key=True)
    name = Column(String, nullable=False)


class Users(base):
    __tablename__ = 'users'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    date_registered = Column(Integer)
    last_seen = Column(Integer)
    timestamp = Column(Integer)
    
    
class Threads(base): 
    __tablename__ = 'threads'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(String, ForeignKey('users.user_id'))
    thread_id = Column(Integer, primary_key=True)
    message = Column(String)
    timestamp = Column(Integer)
    
    
class Replies(base):
    __tablename__ = 'replies'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(String, ForeignKey('users.user_id'))
    reply_id = Column(Integer, primary_key=True)
    message = Column(String)

    timestamp = Column(Integer)
    replynumber = Column(Integer)


class Levels(base):
    __tablename__ = 'levels'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(String, ForeignKey('users.user_id'))
    level_id = Column(Integer, primary_key=True)
    level = Column(Integer)
    timestamp = Column(Integer)    


class Karma(base): 
    __tablename__ = 'karma'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(String, ForeignKey('users.user_id'))
    karma_id = Column(Integer, primary_key=True)
    karma = Column(Integer)
    timestamp = Column(Integer)
    
class Posts(base): 
    __tablename__ = 'posts'
    forum_id = Column(String(4), ForeignKey('forums.forum_id'))
    user_id = Column(String, ForeignKey('users.user_id'))
    post_id = Column(Integer, primary_key=True)
    posts = Column(Integer)
    timestamp = Column(Integer)
    

Session = sessionmaker(db)  
session = Session()
base.metadata.create_all(db)