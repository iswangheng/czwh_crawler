#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 7 Nov, 2012
@author: swarm
'''

import logging
from logging import handlers
from ConfigParser import ConfigParser
import orm
import job_const
import os
import time
import sys
import urllib2
import json
import simplejson

class Producer(object):
    '''
    this Producer class is responsible for producing status_show jobs,
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
    
    
    def start_status_show(self):
        """
        added by Dr. Lei CHEN
        need to put those statuses_ids into the job and then send it.
        """
        job_type = job_const.JOB_TYPE_STATUSES_SHOW
        need_job_num = 150
        print "will produce job"
        while self.produce_job(job_type, need_job_num):
            print 'has produced a job, will send the job'
            self.send_job()
            time.sleep(31)
     
    def produce_job(self, job_type, need_job_num):
        """
        will query the DB and then produce corresponding jobs according to job_type
        @param job_type: has many different type of jobs, see: job_type module 
        @param need_job_num: how many jobs are needed  
        the job dict is like this:
            job = {'job_source': job_const.JOB_SOURCE_JOB_PRODUCER, 
                   'job_type': job_const.JOB_TYPE_FOLLOW,
                   'max_num': 2000, (maybe not)
                   'user_id_list': user_id_list, OR 'statuses_id_list': statuses_id_list,
                  }
        """
        need_to_produce_status_job = False
        limit_num = need_job_num
        #===================================================================
        # job_source has two kinds of values: JOB_SOURCE_JOB_PRODUCER 
        #                                     and JOB_SOURCE_REALTIME_PRODUCER
        #     here,the job_source should always be JOB_SOURCE_JOB_PRODUCER, 
        #          since this is job_producer here
        #===================================================================
        self.job['job_source'] = job_const.JOB_SOURCE_JOB_PRODUCER
        self.job['job_type'] = job_type
        #===============================================================================
        # current job_types:  (refer to the job_const.py)
        #===============================================================================
        if job_type == job_const.JOB_TYPE_STATUSES_SHOW:
            # will produce jobs of the statuses_show
            statuses_id_list = self.query_update_keyword_status(limit_num)
            self.job['statuses_id_list'] = statuses_id_list
            if statuses_id_list != []:
                need_to_produce_status_job = True
                self.logger.info('need more status jobs...')
        else:
            pass
        return need_to_produce_status_job 
    
    def query_update_keyword_status(self, limit_num):
        """
        will query the DB for keyword_status that have not updated the status
        """
        statuses_id_list = [] 
        session = orm.load_session()
        query = session.query(orm.KeywordStatus)
        try:
            keyword_status_list_db = query.filter(orm.KeywordStatus.update_status_time == None).limit(limit_num)
            session.commit()
            for keyword_status_db in keyword_status_list_db:
                print "keyword_status_lit_db not empty, status_id: %s" % (keyword_status_db.status_id)
                statuses_id_list.append(keyword_status_db.status_id)
        except:
            self.logger.error('query update_keyword_status error')
            self.logger.error('%s' % (sys.exc_info()[1]))
        finally:
            session.close()
        return statuses_id_list 
            
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


def print_timing(func):
    """
    used as decorator
    """
    def wrapper(*arg):
        t1 = time.time()
        res = func(*arg)
        t2 = time.time()
        print '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0)
        return res
    return wrapper

@print_timing
def main():
    producer = Producer()
    producer.start_status_show()


if __name__ == '__main__':
    main()
