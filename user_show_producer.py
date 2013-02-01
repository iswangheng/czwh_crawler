#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 7 Nov, 2012
@author: swarm
'''

import logging
from logging import handlers
from ConfigParser import ConfigParser
import job_const
import MySQLdb as mdb
import os
import sys
import urllib2
import json

class Producer(object):
    '''
    this Producer class is responsible for producing user_show jobs,
    would also produce some follow, weibo, and bi_follow jobs 
      just for those Industry Leaders specifically
    and then send the job to crawler_master
    '''

    def __init__(self):
        self.logger = self.set_logger()
        self.config = self.set_config()
        self.crawler_master_url = self.config.get('crawler_master', 'master_url')
        self.job = {}
        pass
    
    def set_logger(self):
        """
        will set up the logger for producer
        """
        logger = logging.getLogger("Producer")
        hdlr = logging.handlers.RotatingFileHandler(filename='./logs/producer.log', maxBytes=20480000, \
                                                        backupCount = 10)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.INFO)
        return logger

    def set_config(self):
        """ will read producer_config.ini and then return the config for further use
        """
        filename = os.path.join('.', 'producer_config.ini')
        config = ConfigParser()
        config.read(filename)
        return config
    
    def start_user_show(self, job_type):
        print "will produce %s job" % (job_type)
        self.produce_job(job_type)
        print 'has produced a job, will send the job'
        self.send_job()
        print 'has sent the %s job' % (job_type)
        
    def produce_job(self, job_type):
        """
        the job dict is like this:
            job = {'job_source': job_const.JOB_SOURCE_JOB_PRODUCER, 
                   'job_type': job_const.JOB_TYPE_FOLLOW,
                   'max_num': 2000, (maybe not)
                   'user_id_list': user_id_list, 
                  }
        """
        self.job['job_source'] = job_const.JOB_SOURCE_JOB_PRODUCER
        self.job['job_type'] = job_type
        #=======================================================================
        # actually max_num is NOT userfull at all here
        # but for consistency, I have to add it below
        #=======================================================================
        self.job['max_num'] = 2000
        #===============================================================================
        # current job_types:  (refer to the job_const.py)
        #===============================================================================
        if job_type == job_const.JOB_TYPE_USER_SHOW:
            # will produce jobs of the user_show
            user_id_list = self.query_industry_user_ids()
            self.job['user_id_list'] = user_id_list 
            print user_id_list
        elif job_type == job_const.JOB_TYPE_FOLLOW:
            user_id_list = self.query_industry_user_follow()
            self.job['user_id_list'] = user_id_list 
            print "The number of industry users(not update follow) is %d" % (len(user_id_list))
        elif job_type == job_const.JOB_TYPE_USER_WEIBO:
            user_id_list = self.query_industry_user_weibo()
            self.job['user_id_list'] = user_id_list 
            print "The number of industry users(not update weibo) is %d" % (len(user_id_list))
        elif job_type == job_const.JOB_TYPE_BI_FOLLOW_ID:
            user_id_list = self.query_industry_user_bi_follow()
            self.job['user_id_list'] = user_id_list 
            print "The number of industry users(not update bi_follow) is %d" % (len(user_id_list))
        else:
            pass
        return 
    
    def query_industry_user_follow(self):
        column_name = "demo_users.update_following_time"
        user_id_list = self.query_db(column_name)
        return user_id_list

    def query_industry_user_weibo(self):
        column_name = "demo_users.update_weibo_time"
        user_id_list = self.query_db(column_name)
        return user_id_list
    
    def query_industry_user_bi_follow(self):
        column_name = "demo_users.update_bi_follow_time"
        user_id_list = self.query_db(column_name)
        return user_id_list
    
    def query_db(self, column_name):
        user_id_list = []
        con = mdb.connect('localhost', 'root', 'swarm', 'sina_weibo')
        cur = con.cursor()
        con.set_character_set('utf8')
        cur.execute('SET NAMES utf8;')
        cur.execute('SET CHARACTER SET utf8;')
        cur.execute('SET character_set_connection=utf8;')
        fetch_str = 'SELECT industry_users.user_id FROM industry_users, demo_users ' + \
                    'WHERE industry_users.user_id = demo_users.user_id ' + \
                    'AND %s is null;' % (column_name) 
        cur.execute(fetch_str)
        rows = cur.fetchall()
        for row in rows:
            user_id_list.append(row[0])
        cur.close()
        con.close()
        return user_id_list
    
    def query_industry_user_ids(self):
        user_id_list = []
        con = mdb.connect('localhost', 'root', 'swarm', 'sina_weibo')
        cur = con.cursor()
        con.set_character_set('utf8')
        cur.execute('SET NAMES utf8;')
        cur.execute('SET CHARACTER SET utf8;')
        cur.execute('SET character_set_connection=utf8;')
        fetch_str = 'SELECT user_id FROM industry_users;' 
        cur.execute(fetch_str)
        rows = cur.fetchall()
        for row in rows:
            user_id_list.append(row[0])
        cur.close()
        con.close()
        return user_id_list
    
    def send_job(self):
        """
        will send the produced job to crawler_master
        """
        print 'job: %s' % (self.job['job_type'])
        post_job_json = json.dumps(self.job)
        post_job_url = self.match_post_job_url(self.job['job_type'])
        try:
            req = urllib2.Request(url=post_job_url, \
                                  data=post_job_json, \
                                  headers={'Content-Type': 'application/json'})
            f = urllib2.urlopen(req)
            response = f.read()
            print response
        except:
            self.logger.error('send_job() error, maybe urllib2(request, open, read, etc.)')
    
    def match_post_job_url(self, job_type):
        post_job_url = self.crawler_master_url + 'follow/'
        if job_type == job_const.JOB_TYPE_FOLLOW:
            post_job_url = self.crawler_master_url + 'follow/'
        elif job_type ==  job_const.JOB_TYPE_BI_FOLLOW_ID:
            post_job_url = self.crawler_master_url + 'bi_follow_id/'
        elif job_type == job_const.JOB_TYPE_USER_WEIBO:
            post_job_url = self.crawler_master_url + 'user_weibo/'
        elif job_type == job_const.JOB_TYPE_STATUSES_SHOW:
            post_job_url = self.crawler_master_url + 'statuses_show/'
        elif job_type == job_const.JOB_TYPE_USER_SHOW:
            post_job_url = self.crawler_master_url + 'user_show/'
        #===============================================================================
        # add new job_types processing here
        # for current job_types please refer to the job_const.py
        #===============================================================================
        else:
            pass
        return post_job_url
        
    
def map_rowobject_dict(row_obj):
    """
    convert the sqlalchemy object to dict in python..
    """
    py_dict = {}
    for column in row_obj.__table__.columns:
        py_dict[column.name] = getattr(row_obj, column.name)
    return py_dict


def main():
    producer = Producer()
#    job_type = job_const.JOB_TYPE_FOLLOW
#    job_type = job_const.JOB_TYPE_USER_WEIBO
    job_type = job_const.JOB_TYPE_BI_FOLLOW_ID
#    producer.start_user_show(job_type)


if __name__ == '__main__':
    main()
