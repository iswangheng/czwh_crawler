#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 8 Nov, 2012
@author: swarm
'''

import web
import json
import orm
import math
import sys
from dateutil import parser
from sqlalchemy import exc
from collections import defaultdict
import datetime
import job_const
import operator
import logging
from config import settings

logger = logging.getLogger("crawler_master")
db = settings.db


def handle_bi_follow_id(crawler_json):
    """
    will take the json object returned from the crawler as input
    and then store corresponding part into the DB
    """
    user_id = crawler_json['user_id']
    sina_weibo_json = crawler_json['sina_weibo_json']
    bi_follow_id_list = sina_weibo_json['ids']
#    store_bi_follow_id(user_id, bi_follow_id_list)
    new_store_bi_follow_id(user_id, bi_follow_id_list)

def handle_follow(crawler_json):
    """
    will take the json object returned from the crawler as input
    and then store corresponding part into the DB
    """
    user_id = crawler_json['user_id']
    follow_json_list = crawler_json['sina_weibo_json']
    for follow_json in follow_json_list:
        follow_list = follow_json['users']
        store_follow_list(user_id, follow_list)
        #=======================================================================
        # looks like the new_store_follow_list() is TOO SLOW
        # thus will not be used.... edited by swarm @ 2013 Jan 29th.
        #=======================================================================
#        new_store_follow_list(user_id, follow_list)

def handle_user_weibo(crawler_json):
    """
    will take the json object returned from the crawler as input
    and then store corresponding part into the DB
    """
    user_id = crawler_json['user_id']
    statuses_list = crawler_json['sina_weibo_json']
    session = orm.load_session()
    try:
        for status in statuses_list:
            store_status(status, session)
        session.commit()
    except exc.SQLAlchemyError, e:
        logger.error(e)
        session.rollback()
    else:
        try:
            #===========================================================================
            # will update the update_weibo_time column of the user table
            #===========================================================================
            update_weibo_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            session.query(orm.DemoUsers).filter_by(user_id=user_id). \
                  update({"update_weibo_time": update_weibo_time}, synchronize_session=False)
            session.commit()
        except exc.SQLAlchemyError, e:
            logger.error(e)
            session.rollback()
    finally:
        session.close()

def handle_statuses_show(crawler_json):
    """
    will take the json object returned from the crawler as input
    and then store corresponding part into the DB
    """
    statuses_list = crawler_json['sina_weibo_json_list']
    session = orm.load_session()
    try:
        for status in statuses_list:
            if status['exist']:
                store_status(status, session)
            #===========================================================================
            # will update the update_status_time column of the keyword_status table
            #===========================================================================
            update_status_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if status.has_key('user'):
                user_obj = status['user']
                store_user_session(user_obj, session)
            session.query(orm.KeywordStatus).filter_by(status_id=status['id']). \
                         update({"update_status_time": update_status_time}, synchronize_session=False)
        session.commit()
    except exc.SQLAlchemyError, e:
        logger.error(e)
        session.rollback()
    finally:
        session.close()
        

def handle_keyword_status_ids(keyword, status_id_list):
    """
    store the keyword and corresponding status_ids into DB
    """
    logger.info("okay, will handle_keyword_status_ids(keyword, status_id_list)")
    logger.info("status_id_list is %d length" % (len(status_id_list)))
    session = orm.load_session()
    result = True
    try:
        for status_id in status_id_list:
            store_keyword_status_id(keyword, status_id, session)
        session.commit()
        logger.info("successfully committed the keyword_stauts_id")
    except exc.SQLAlchemyError, e:
        logger.error(e)
        session.rollback()
        result = False
    except:
        logger.error("handle_keyword_status_ids() unexpected error")
        result = False
    finally:
        session.close()
        return result


def has_stored_user_by_uid(user_id):
    """
    will query the DB, table "demo_users" , and then decide whether has stored this user or not
    @param user_id: id of the user 
    @return: has_stored is a binary value which indicates that whether the user has stored or not
    """
    session = orm.load_session()
    has_stored = False
    query = session.query(orm.DemoUsers)
    count = query.filter(orm.DemoUsers.user_id == user_id).count()
    session.commit()
    if count != 0:
        has_stored = True
    session.close()
    return has_stored

def add_orm_user(user):
    """
    just convert the user object(returned by SinaWeiboAPI) to orm object
    @param user: user object(returned by SinaWeiboAPI) 
    @return:  add_orm_user, for ORM use
    """
    created_at_time = parser.parse(user['created_at'])
    created_at = created_at_time.strftime("%Y-%m-%d %H:%M:%S")
    update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    add_orm_user = None
    try:
        add_orm_user = orm.DemoUsers(user_id=user['id'], name=user['name'], \
                         screen_name=user['screen_name'], province=user['province'], \
                         city=user['city'], location=user['location'], \
                         description=user['description'], url=user['url'], \
                         profile_image_url=user['profile_image_url'], domain=user['domain'], \
                         gender=user['gender'], followers_count=user['followers_count'], \
                         friends_count=user['friends_count'], statuses_count=user['statuses_count'], \
                         favourites_count=user['favourites_count'], created_at=created_at, \
                         allow_all_act_msg=user['allow_all_act_msg'], geo_enabled=user['geo_enabled'], \
                         verified=user['verified'], allow_all_comment=user['allow_all_comment'], \
                         avatar_large=user['avatar_large'], verified_reason=user['verified_reason'], \
                         bi_followers_count=user['bi_followers_count'], \
                         tags='', update_time=update_time, has_enough_friends_stored=0,  \
                         update_following_time=None, update_bi_follow_time=None,   \
                         update_weibo_time=None
                         )
    except:
        add_orm_user = None
        logger.error("what the fuck add_orm_user error again !!!!!!!!!!1")
        logger.error('%s %s ' % (sys.exc_info()[0], sys.exc_info()[1]))
    finally:
        return add_orm_user

def update_user(user, session):
    """
    this is to update the existing user in the table,
    @param user:  the user object
    @param session:  the session concept in ORM(SqlAlchemy)
    @attention: WITHOUT the session.commit()
    """
    update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        session.query(orm.DemoUsers).filter_by(user_id=user['id']). \
            update({"name": user['name'], "screen_name":user['screen_name'], \
                   "province": user['province'], "city": user['city'], \
                   "location": user['location'], "description": user['description'], \
                   "url": user['url'], "profile_image_url": user['profile_image_url'], \
                   "domain": user['domain'], "gender": user['gender'], \
                   "followers_count": user['followers_count'], "friends_count": user['friends_count'], \
                   "statuses_count": user['statuses_count'], "favourites_count": user['favourites_count'], \
                   "allow_all_act_msg": user['allow_all_act_msg'], "geo_enabled": user['geo_enabled'], \
                   "verified": user['verified'], "allow_all_comment": user['allow_all_comment'], \
                   "avatar_large": user['avatar_large'], "verified_reason": user['verified_reason'], \
                   "bi_followers_count": user['bi_followers_count'], "update_time": update_time, \
                   }, \
                   synchronize_session=False \
                  )
    except:
        logger.error('update_user() error..')
        logger.error('%s %s ' % (sys.exc_info()[0], sys.exc_info()[1]))

def add_orm_status(status):
    """
    just convert the status object(returned by SinaWeiboAPI) to orm object
    @param status: status object(returned by SinaWeiboAPI) 
    @return:  add_orm_status, for ORM use
    """
    created_at_time = parser.parse(status['created_at'])
    created_at = created_at_time.strftime("%Y-%m-%d %H:%M:%S")
    source = ''
    retweeted_status_id=0
    original_pic=''
    geo=''
    if status.has_key('source'):
        source = status['source']
    if status.has_key('retweeted_status'):
        retweeted_status_id = status['retweeted_status']['id']
    if status.has_key('original_pic'):
        original_pic = status['original_pic']
    if status.has_key('geo'):
#        geo = str(status['geo'])
        geo = ""
    add_orm_status = None
    try:
        add_orm_status = orm.Statuses(status_id=status['id'], created_at=created_at, \
                      text=status['text'], source=source,  \
                      in_reply_to_status_id=0,  \
                      in_reply_to_user_id=0,  \
                      in_reply_to_screen_name=0,  \
                      geo=geo, reposts_count=status['reposts_count'], \
                      comments_count=status['comments_count'], \
                      attitudes_count=status['attitudes_count'], user_id=status['user']['id'], \
                      retweeted_status_id=retweeted_status_id, original_pic=original_pic)
    except:
        logger.error("what the fuck add orm status error again !!!!!!!!!!1")
        logger.error('%s %s ' % (sys.exc_info()[0], sys.exc_info()[1]))
    finally:
        return add_orm_status

def update_status(status, session):
    """
    this is to update the existing status in the table,
    @param status:  the status object
    @param session:  the session concept in ORM(SqlAlchemy)
    @attention: WITHOUT the session.commit()
    """
    try:
        session.query(orm.Statuses).filter_by(status_id=status['id']). \
            update({"reposts_count": status['reposts_count'], \
                  "comments_count": status['comments_count'], \
                  "attitudes_count": status['attitudes_count'], \
                   }, \
                   synchronize_session=False \
                  )
    except:
        logger.error('update_status() error..')

def store_user(user):
    """
    store the user object into user table
       if already in DB, then will update the existing one
    """
    try:
        session = orm.load_session()
        demo_user = session.query(orm.DemoUsers).filter_by(user_id=user['id']).first()
        # now will store the user into DB
        if not demo_user:
            # if not in DB, then store into DB 
            add_user = add_orm_user(user)
            if add_user != None:
                session.add(add_user)
        else:
            logger.info("Update this user %s in DB" % user['id'])
            # if in DB, then update the user in DB
            update_user(user, session)
        session.commit()
    except exc.SQLAlchemyError, e:
        logger.error(e)
        session.rollback()
    finally:
        session.close()
    
def store_user_session(user, session):
    """
    store the user object into user table
       if already in DB, then will update the existing one
    """
    try:
        demo_user = session.query(orm.DemoUsers).filter_by(user_id=user['id']).first()
        # now will store the user into DB
        if not demo_user:
            # if not in DB, then store into DB 
            add_user = add_orm_user(user)
            if add_user != None:
                session.add(add_user)
                session.commit()
        else:
            logger.info("Update this user %s in DB" % user['id'])
            # if in DB, then update the user in DB
            update_user(user, session)
    except exc.SQLAlchemyError, e:
        logger.error(e)
        session.rollback()
    
def store_status(status, session):
    """
    store the status object into status table
       if already in DB, then update the existing one
    """
    try:
        orm_status = session.query(orm.Statuses).filter_by(status_id=status['id']).first()
        # now will store the weibo into DB
        if not orm_status:
            # if not in DB, then store into DB 
            add_status = add_orm_status(status)
            if add_status != None:
                session.add(add_status)
            if status.has_key('retweeted_status'):
                retweeted_status = status['retweeted_status']
                store_status(retweeted_status, session)
        else:
            logger.info("Update this weibo %s in DB" % status['id'])
            # if in DB, then update the user in DB
            update_status(status, session)
    except exc.SQLAlchemyError, e:
        logger.error(e)
    
def store_follow_list(user_id, follow_list):
    """
    just store the user's followings 
        (both the following relationship and the following user into user table) into DB
    @param user_id: id of the user
    @param follow_list: a list of the followings of the user
                    here the element in the follow_list is the user object returned by SinaWeiboAPI
    """
    try:
        session = orm.load_session()
        for user in follow_list:
            demo_user = session.query(orm.DemoUsers).filter_by(user_id=user['id']).first()
            # now will store the following into db
            if not demo_user:
                # if not in DB, then store into DB 
                add_user = add_orm_user(user)
                session.add(add_user)
            else:
                logger.info("this following %s is already in DB"  % user['id'])
                logger.info("Update this following user %s in DB" % user['id'])
                # if in DB, then update the user in DB
                update_user(user, session)
            # now will store the follow relationship into db
            following_id = user['id']
            follow = session.query(orm.Follow).filter_by(user_id=user_id, following_id=following_id).first()
            if not follow:
                # if not in DB, then store into DB 
                add_follow = orm.Follow(user_id=user_id, following_id=following_id)
                session.add(add_follow)
            else:
                logger.info("%s -> %s already in DB" % (user_id, following_id))
    except:
        error_str = 'store_follow_list %s %s' % (sys.exc_info()[0], sys.exc_info()[1])
        logger.error(error_str)
    else:
        try:
            #===========================================================================
            # will update the update_following_time column of the user table
            #===========================================================================
            update_following_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            session.query(orm.DemoUsers).filter_by(user_id=user_id). \
                  update({"update_following_time": update_following_time}, synchronize_session=False)
            session.commit()
        except exc.SQLAlchemyError, e:
            logger.error(e)
            session.rollback()
        except:
            logger.error("maybe update_following_time error")
    finally:
        session.close()

def db_insert_user(user, db, db_transaction):
    """
    @attention: 
        db_transaction.commit()
        is omitted here, Remember to add this sentence in other place
    @param user: user object(returned by SinaWeiboAPI) 
    @return:  insert_result(True|False)
    """
    insert_result = False
    try:
        created_at_time = parser.parse(user['created_at'])
        created_at = created_at_time.strftime("%Y-%m-%d %H:%M:%S")
        update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db.insert('demo_users', user_id=user['id'], name=user['name'],   \
                  screen_name=user['screen_name'], province=user['province'], \
                  city=user['city'], location=user['location'], \
                  description=user['description'], url=user['url'], \
                  profile_image_url=user['profile_image_url'], domain=user['domain'], \
                  gender=user['gender'], followers_count=user['followers_count'], \
                  friends_count=user['friends_count'], statuses_count=user['statuses_count'], \
                  favourites_count=user['favourites_count'], created_at=created_at, \
                  allow_all_act_msg=user['allow_all_act_msg'], geo_enabled=user['geo_enabled'], \
                  verified=user['verified'], allow_all_comment=user['allow_all_comment'], \
                  avatar_large=user['avatar_large'], verified_reason=user['verified_reason'], \
                  bi_followers_count=user['bi_followers_count'], \
                  tags='', update_time=update_time, has_enough_friends_stored=0,  \
                  update_following_time=None, update_bi_follow_time=None,   \
                  update_weibo_time=None
                  )
    except:
        db_transaction.rollback()
        logger.info("So %s already in DB" % (user['id']))
    else:
        insert_result = True
    return insert_result

#===============================================================================
# A NEW version of store_follow_list(user_id, follow_list)
# use web.py.db.insert directly 
#===============================================================================
def new_store_follow_list(user_id, follow_list):
    """
    just store the user's followings 
        (both the following relationship and the following user into user table) into DB
    @param user_id: id of the user
    @param follow_list: a list of the followings of the user
                    here the element in the follow_list is the user object returned by SinaWeiboAPI
    """
    logger.info("okay now in new_store_follow_list")
    db_transaction = db.transaction()
    try:
        session = orm.load_session()
        for user in follow_list:
            following_id = user['id']
            # store the user into DB 
            if not db_insert_user(user, db, db_transaction):
                # means already in DB, then update user DB 
                logger.info("this following %s is already in DB"  % following_id)
                logger.info("Update this following user %s in DB" % following_id)
                update_user(user, session)
            # now will store the follow relationship into db
            try:
                db.insert('follow', user_id=user_id, following_id=following_id)
            except:
                db_transaction.rollback()
                logger.error("new_store_follow_list db.insert follow table error. DUPLICATE?..")
                logger.info("So %s -> %s already in DB" % (user_id, following_id))
    except:
        error_str = 'new_store_follow_list %s %s' % (sys.exc_info()[0], sys.exc_info()[1])
        logger.error(error_str)
    else:
        # the reason why put commit() here is just to improve the speed of insert
        db_transaction.commit()
        try:
            #===========================================================================
            # will update the update_following_time column of the user table
            #===========================================================================
            update_following_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            session.query(orm.DemoUsers).filter_by(user_id=user_id). \
                  update({"update_following_time": update_following_time}, synchronize_session=False)
            session.commit()
        except exc.SQLAlchemyError, e:
            logger.error(e)
            session.rollback()
        except:
            logger.error("maybe update_following_time error")
    finally:
        session.close()


def store_bi_follow_id(user_id, bi_follow_id_list):
    """
    store the user's bi_follow_id into the bi_follow table
    @param user_id: id of the user
    @param bi_follow_id_list: a list of bi_follow_id of the user
    """
    try:
        session = orm.load_session()
        for bi_following_id in bi_follow_id_list:
            # now will store the bi_follow relationship into db
            bi_follow = session.query(orm.BiFollow).filter_by(user_id=user_id, bi_following_id=bi_following_id).first()
            if not bi_follow:
                # if not in DB, then store into DB 
                add_bi_follow = orm.BiFollow(user_id=user_id, bi_following_id=bi_following_id)
                session.add(add_bi_follow)
            else:
                logger.info("%s <-> %s already in DB" % (user_id, bi_following_id))
    except:
        logger.error("store_bi_follow_id error.. session.add? i do NOT know yet")
        logger.error('%s %s ' % (sys.exc_info()[0], sys.exc_info()[1]))
    else:
        try:
            #===========================================================================
            # will update the update_bi_follow_time column of the user table
            #===========================================================================
            update_bi_follow_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            session.query(orm.DemoUsers).filter_by(user_id=user_id). \
                  update({"update_bi_follow_time": update_bi_follow_time}, synchronize_session=False)
            session.commit()
        except exc.SQLAlchemyError, e:
            session.rollback()
            logger.error(e)
    finally:
        session.close()

#===============================================================================
# A NEW version of store_bi_follow_id(user_id, bi_follow_id_list)
# Use web.db directly instead of ORM(SQLAlchemy)
# Insert many records at one time now
#===============================================================================
def new_store_bi_follow_id(user_id, bi_follow_id_list):
    """
    store the user's bi_follow_id into the bi_follow table
    @param user_id: id of the user
    @param bi_follow_id_list: a list of bi_follow_id of the user
    """
    db_transaction = db.transaction()
    try:
        for bi_following_id in bi_follow_id_list:
            # now will store the bi_follow relationship into db
            db.insert('bi_follow', user_id=user_id, bi_following_id=bi_following_id)
    except:
        db_transaction.rollback()
        logger.error("new_store_bi_follow_id ERROR..")
    else:
        db_transaction.commit()
        # will update the update_bi_follow_time column of the user table
        update_bi_follow_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            where = 'user_id=%s' % (str(user_id))
            db.update('demo_users', where=where, update_bi_follow_time=update_bi_follow_time)
        except:
            db_transaction.rollback()
            logger.error("update_bi_follow_time new_store_bi_follow_id ERROR..")
        else:
            db_transaction.commit()
            
        
def store_keyword_status_id(keyword, status_id, session):
    """
    store the keyword and corresponding status_id into the keyword_status table in DB
    @param keyword: 
    @param status_id: 
    @param session: the session of SQLAlchemy 
    """
    #===========================================================================
    # class KeywordStatus(Base):
    #    __tablename__ = 'keyword_status'
    #    status_id = Column(BIGINT, primary_key = True)
    #    keyword = Column(VARCHAR)
    #    update_status_time = Column(DATETIME)
    #    text = Column(VARCHAR)
    #    word_seg = Column(VARCHAR)
    #    tags_extracted = Column(VARCHAR)
    #    pos_neg = Column(VARCHAR)
    #===========================================================================
    keyword_status = session.query(orm.KeywordStatus).filter_by(status_id=status_id).first()
    if not keyword_status:
        add_keyword_status = orm.KeywordStatus(status_id=status_id, \
                                               keyword=keyword, \
                                               update_status_time=None, \
                                               text=None, \
                                               word_seg=None, \
                                               tags_extracted=None, \
                                               pos_neg=None, \
                                               )
        session.add(add_keyword_status)
        if_str = "store the status %s into keyword_status table" % str(status_id)
        logger.info(if_str)
        print if_str
    else:
        else_str = "%s already in keyword_status table" % str(status_id)
        logger.info(else_str)
        print else_str