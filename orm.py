#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
@author: swarm
"""

import logging
import os
from ConfigParser import ConfigParser
from sqlalchemy import create_engine, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import \
        BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, DATE, \
        DATETIME, DECIMAL, DECIMAL, DOUBLE, ENUM, FLOAT, INTEGER, \
        LONGBLOB, LONGTEXT, MEDIUMBLOB, MEDIUMINT, MEDIUMTEXT, NCHAR, \
        NUMERIC, NVARCHAR, REAL, SET, SMALLINT, TEXT, TIME, TIMESTAMP, \
        TINYBLOB, TINYINT, TINYTEXT, VARBINARY, VARCHAR, YEAR

filename = os.path.join('.', 'producer_config.ini')
config = ConfigParser()
config.read(filename)
host = config.get('database','host')
user = config.get('database','user')
passwd = config.get('database','passwd')
db = config.get('database','db')
engine_str = "mysql://%s:%s@%s/%s?charset=utf8" % (user, passwd, host, db)

engine = create_engine(engine_str, encoding="utf8", convert_unicode=True, echo=False, pool_recycle=3600)
Base = declarative_base(engine)

def set_dblogger():
    logger = logging.getLogger('sqlalchemy.engine')
    hdlr = logging.handlers.RotatingFileHandler(filename='./logs/db_orm.log', maxBytes=20480000, \
                                                    backupCount = 10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)


class DemoUsers(Base):
    __tablename__ = 'demo_users'
    user_id = Column(BIGINT, primary_key = True)
    name = Column(VARCHAR)
    screen_name = Column(VARCHAR)
    province = Column(INTEGER)
    city = Column(INTEGER)
    location = Column(VARCHAR)
    description = Column(VARCHAR)
    url = Column(VARCHAR)
    profile_image_url = Column(VARCHAR)
    domain = Column(VARCHAR)
    gender = Column(VARCHAR)
    followers_count = Column(BIGINT)
    friends_count = Column(INTEGER)
    statuses_count = Column(INTEGER)
    favourites_count = Column(INTEGER)
    created_at = Column(DATETIME)
    allow_all_act_msg = Column(TINYINT)
    geo_enabled = Column(TINYINT)
    verified = Column(TINYINT)
    allow_all_comment = Column(TINYINT)
    avatar_large = Column(VARCHAR)
    verified_reason = Column(VARCHAR)
    bi_followers_count = Column(INTEGER)
    tags = Column(VARCHAR)
    update_time = Column(DATETIME)
    has_enough_friends_stored = Column(TINYINT)
    update_following_time = Column(DATETIME)
    update_bi_follow_time = Column(DATETIME)

    def __init__(self, user_id, name, screen_name, province, \
                 city, location, description, url, \
                 profile_image_url, domain, gender, followers_count, \
                 friends_count, statuses_count, favourites_count, created_at, \
                 allow_all_act_msg, geo_enabled, verified, allow_all_comment, \
                 avatar_large, verified_reason, bi_followers_count, \
                 tags, update_time, has_enough_friends_stored,  \
                 update_following_time, update_bi_follow_time):
        self.user_id = user_id
        self.name = name
        self.screen_name = screen_name
        self.province = province
        self.city = city
        self.location = location
        self.description = description
        self.url = url 
        self.profile_image_url = profile_image_url
        self.domain = domain
        self.gender = gender
        self.followers_count = followers_count
        self.friends_count = friends_count
        self.statuses_count = statuses_count
        self.favourites_count = favourites_count
        self.created_at = created_at
        self.allow_all_act_msg = allow_all_act_msg
        self.geo_enabled = geo_enabled
        self.verified = verified
        self.allow_all_comment = allow_all_comment
        self.avatar_large = avatar_large
        self.verified_reason = verified_reason
        self.bi_followers_count = bi_followers_count
        self.tags = tags 
        self.update_time = update_time
        self.has_enough_friends_stored = has_enough_friends_stored
        self.update_following_time = update_following_time
        self.update_bi_follow_time = update_bi_follow_time
        pass

    def __repr__(self):
        """
        will return a pretty string of current object
        """
        return "<DemoUser- '%s' - '%s'- '%s' \
                - '%s' - '%s' - '%s' - '%s' - '%s' \
                - '%s' - '%s' - '%s' - '%s' - '%s'  \
                - '%s'- '%s' - '%s'  - '%s' - '%s'   \
                - '%s'- '%s' - '%s'  - '%s' - '%s'   \
                - '%s'- '%s' - '%s'  - '%s' - '%s'>"  \
                (self.user_id, self.name, self.screen_name, \
                self.province, self.city, self.location, self.description, self.url,
                self.profile_image_url, self.domain, self.gender, self.followers_count, self.friends_count,
                self.statuses_count, self.favourites_count, self.created_at, self.allow_all_act_msg, 
                self.geo_enabled, self.verified, self.allow_all_comment, self.avatar_large,
                self.verified_reason, self.bi_followers_count, self.tags, self.update_time, 
                self.has_enough_friends_stored, self.update_following_time, self.update_bi_follow_time)


class InferUsers(Base):
    __tablename__ = 'infer_users'
    user_id = Column(BIGINT, primary_key = True)
    gender = Column(VARCHAR)
    age = Column(VARCHAR)
    career = Column(VARCHAR)
    tags = Column(VARCHAR)

    def __init__(self, user_id, gender, age, career, tags):
        self.user_id = user_id
        self.gender = gender 
        self.age = age
        self.career = career
        self.tags = tags

    def __repr__(self):
        return "<InferUsers- '%s' - '%s'- '%s' \
                - '%s'- '%s'>"  \
              % (self.user_id, self.gender, self.age, \
               self.career, self.tags)
    
class Statuses(Base):
    __tablename__ = 'statuses'
    status_id = Column(BIGINT, primary_key = True)
    created_at = Column(DATETIME, index=True)
    text = Column(VARCHAR)
    source = Column(VARCHAR)
    in_reply_to_status_id = Column(VARCHAR)
    in_reply_to_user_id = Column(VARCHAR)
    in_reply_to_screen_name = Column(VARCHAR)
    geo = Column(VARCHAR)
    reposts_count = Column(INTEGER, index=True)
    comments_count = Column(BIGINT, index=True)
    attitudes_count = Column(INTEGER)
    user_id = Column(BIGINT, index=True)
    retweeted_status_id = Column(BIGINT, index=True)
    original_pic = Column(VARCHAR)

    def __init__(self, status_id, created_at, text, source,  \
                 in_reply_to_status_id, in_reply_to_user_id, in_reply_to_screen_name, \
                 geo, reposts_count, comments_count, attitudes_count,  \
                 user_id, retweeted_status_id, original_pic):
        self.status_id = status_id
        self.created_at = created_at
        self.text = text
        self.source = source
        self.in_reply_to_status_id = in_reply_to_status_id
        self.in_reply_to_user_id = in_reply_to_user_id
        self.in_reply_to_screen_name = in_reply_to_screen_name
        self.geo = geo
        self.reposts_count = reposts_count
        self.comments_count = comments_count
        self.attitudes_count = attitudes_count 
        self.user_id = user_id 
        self.retweeted_status_id = retweeted_status_id
        self.original_pic = original_pic

    def __repr__(self):
        return "<Statuses - '%s' - '%s' - '%s' - '%s' - '%s'>" % \
                (self.status_id, self.created_at, self.text, self.source, self.user_id)
    
class BiFollow(Base):
    __tablename__ = 'bi_follow'
    user_id = Column(BIGINT, primary_key = True)
    bi_following_id = Column(BIGINT, primary_key = True)

    def __init__(self, user_id, bi_following_id):
        self.user_id = user_id
        self.bi_following_id = bi_following_id

    def __repr__(self):
        return "<BiFollow- '%s' -> '%s'>" % (self.user_id, self.bi_following_id)

class Follow(Base):
    __tablename__ = 'follow'
    user_id = Column(BIGINT, primary_key = True)
    following_id = Column(BIGINT, primary_key = True)

    def __init__(self, user_id, following_id):
        self.user_id = user_id
        self.following_id = following_id

    def __repr__(self):
        return "<Follow- '%s' -> '%s'>" % (self.user_id, self.following_id)

def load_session():
    """
    """
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

if __name__ == "__main__":
    set_dblogger()
    session = load_session()
    query = session.query(DemoUsers)
    count = query.filter(DemoUsers.user_id == "2696168031").count()
    print count
    session.commit()