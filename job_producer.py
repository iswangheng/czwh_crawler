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
import job_type
import os
import time
import urllib2
import json
import simplejson

class Producer(object):
    '''
    this Producer class is responsible for producing jobs,
    and then send the job to crawler_master
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.logger = self.set_logger()
        self.config = self.set_config()
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
        """ will read config.ini and then return the config for further use
        """
        filename = os.path.join('.', 'producer_config.ini')
        config = ConfigParser()
        config.read(filename)
        return config
    
    def start(self):
        """
        @todo: should periodically ask the crawler_master for the length of job_queque,
                if the crawler_master needs more jobs, then will produce jobs and then post the jobs to crawler_master
                else:  do nothing, just wait
        """
        while 1:
            no_need_produce = True
            for key, value in self.get_job_queue_length().items():
                if value <= 20:
                    job_type = key
                    need_job_num = 100 - value
                    no_need_produce = False
                    self.logger.info('job_type:%s only has only %s jobs, needs more' % (key, value))
                    self.produce_job(job_type, need_job_num)
                    self.send_job()
            if no_need_produce:
                #@todo:  change this mins to control how many mins the produce will wait..
                mins = 0.05
                sleep_seconds = 60 * mins
                self.logger.info('no need to produce, wait for a few secs')
                print ('no need to produce, wait for a few secs')
                time.sleep(sleep_seconds)
            # sleep for a few seconds after sending jobs, no need to hurry..
            time.sleep(2)
        pass
     
    def produce_job(self, job_type, need_job_num):
        """
        will query the DB and then produce corresponding jobs according to job_type
        @param job_type: has many different type of jobs, see: job_type module 
        @param need_job_num: how many jobs are needed  
        the job dict is like this:
            job = {'job_source': 'job_producer', 
                   'job_type': '1',
                   'user_id_list': user_id_list,
                  }
        """
        limit_num = need_job_num
        #===================================================================
        # job_souce has two kinds of values: 0 and 1
        #      '0'   <----->   job_producer
        #      '1'   <----->   realtime_producer 
        #     here,  the job_source should always be '0', 
        #         since this is job_producer here
        #===================================================================
        self.job['job_source'] = '0'
        self.job['job_type'] = job_type
        if job_type == '1':
            #job_type == '1' means that we should produce jobs about the follow of the users
            user_id_list = self.query_update_following(limit_num)
            self.job['user_id_list'] = user_id_list
            pass
        elif job_type == '2':
            #job_type == '2' means that we should produce jobs of the bi_follow_id of the users
            user_id_list = self.query_update_bi_follow(limit_num)
            self.job['user_id_list'] = user_id_list
            pass
        else:
            pass
        pass
    
    def get_job_queue_length(self):
        """
        will ask the crawler_master for the length of job_queue
        the length is actually a python dict type, which stores the queue lengh of different queues
        @return: job_queue_length, something like this: {'1': 10, '2': 10}
        """
        job_queue_url = 'http://csz908.cse.ust.hk/crawler_master/job_queue/'
        req = urllib2.Request(job_queue_url)
        r = urllib2.urlopen(req)
        job_queue_length = simplejson.load(r)
        return job_queue_length
    
    def query_update_following(self, limit_num):
        """
        will query the DB for users that have not updated their followings
        """
        user_id_list = [] 
        session = orm.load_session()
        query = session.query(orm.DemoUsers)
        try:
            user_list_db = query.filter(orm.DemoUsers.update_following_time == None).limit(limit_num)
            for user_db in user_list_db:
                user = map_rowobject_dict(user_db)
                user_id_list.append(user['user_id'])
            session.commit()
        except:
            self.logger.error('query_update_following error')
        finally:
            session.close()
        return user_id_list
    
    def query_update_bi_follow(self, limit_num):
        """
        will query the DB for users that have not updated their bi_follow
        """
        user_id_list = [] 
        session = orm.load_session()
        query = session.query(orm.DemoUsers)
        try:
            user_list_db = query.filter(orm.DemoUsers.update_bi_follow_time == None).limit(limit_num)
            for user_db in user_list_db:
                user = map_rowobject_dict(user_db)
                user_id_list.append(user['user_id'])
            session.commit()
        except:
            self.logger.error('query_update_bi_follow error')
        finally:
            session.close()
        return user_id_list
    
    def send_job(self):
        """
        will send the produced job to crawler_master
        """
        print 'job: %s, user id nums: %s' % \
               (job_type.job_dict_inverse[self.job['job_type']], len(self.job['user_id_list']))
        post_job_json = json.dumps(self.job)
        post_job_url = 'http://csz908.cse.ust.hk/crawler_master/follow/'
        if self.job['job_type'] == '1':
            post_job_url = 'http://csz908.cse.ust.hk/crawler_master/follow/'
        elif self.job['job_type'] == '2':
            post_job_url = 'http://csz908.cse.ust.hk/crawler_master/bi_follow_id/'
        #===============================================================================
        # add new job_types processing here
        # current job_types:
        #    1   <------>   follow
        #    2   <------>   bi_follow_id
        #===============================================================================
        elif self.job['job_type'] == '3':
#            post_job_url = 'http://csz908.cse.ust.hk/crawler_master/bi_follow_id/'
            pass
        try:
            req = urllib2.Request(url=post_job_url, \
                                  data=post_job_json, \
                                  headers={'Content-Type': 'application/json'})
            f = urllib2.urlopen(req)
            response = f.read()
            print response
        except:
            self.logger.error('send_job() error, maybe urllib2(request, open, read, etc.)')
        finally:
            f.close()
        pass
    
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
    producer.start()


if __name__ == '__main__':
    main()
