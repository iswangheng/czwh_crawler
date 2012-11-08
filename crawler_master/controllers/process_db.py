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
import operator
import logging

logger = logging.getLogger("crawler_master")

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
                     update_following_time=None, update_bi_follow_time=None
                     )
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

def store_user(user):
    """
    store the user object into user table
       if already in DB, then will update the existing one
    """
    try:
        session = orm.load_session()
        demo_user = session.query(orm.DemoUsers).filter_by(user_id=user['id']).first()
        # now will store the following into DB
        if not demo_user:
            # if not in DB, then store into DB 
            add_user = add_orm_user(user)
            session.add(add_user)
        else:
            logger.info("Update this user %s in DB" % user['id'])
            # if in DB, then update the user in DB
            update_user(user, session)
        session.commit()
    except exc.SQLAlchemyError, e:
        logger.error(e)
    finally:
        session.close()
    
           
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
        logger.error("store_follow_list error.. session.add? i do NOT know yet")
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
        except:
            logger.error("maybe update_following_time error")
    finally:
        session.close()
    pass


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
            logger.error(e)
    finally:
        session.close()