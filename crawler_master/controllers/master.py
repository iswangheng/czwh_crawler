#!usr/bin/env python
# coding: utf-8

import web
import json
import sys
import math
import shlex
import logging
import job_const
import process_db
import time
import signal
import datetime
import redis
import operator
from time import gmtime, strftime
from config import settings

logger = logging.getLogger("crawler_master")
crawler_version = settings.crawler_version
render = settings.render
config = settings.config
db = settings.db
master_redis = settings.master_redis


def push_job_queue(job_json):
    """
    this is to push the job_json into the right/correct/corresponding job_queue
    @param job_json: each job_json contains the job_type and a series of jobs(user_id).. 
    """
    try:
        for user_id in job_json['user_id_list']:
            job_id = master_redis.incr(job_const.JOB_ID)
            queue_job = {'job_id': job_id}
            queue_job.update({'job_type': job_json['job_type']})
            queue_job.update({'job_source': job_json['job_source']})
            queue_job.update({'user_id': user_id})
            queue_job_json_str = json.dumps(queue_job)
            #===================================================================
            # job_source has two kinds of values: JOB_SOURCE_JOB_PRODUCER 
            #                                     and JOB_SOURCE_REALTIME_PRODUCER
            #      if job_source == JOB_SOURCE_REALTIME_PRODUCER, 
            #        means that this is an urgent job,
            #        so we need the crawler to process this job ASAP,
            #        thus the Redis should left push the job into JOB_URGENT_QUEUE
            #===================================================================
            if job_json['job_source'] == job_const.JOB_SOURCE_REALTIME_PRODUCER:
                master_redis.lpush(job_const.JOB_URGENT_QUEUE, queue_job_json_str)
            else:
                master_redis.rpush(job_json['job_type'], queue_job_json_str)
        return queue_job
    except:
        error_str = ('Error of push_job_queue %s' % (sys.exc_info()[0]))
        logger.error(error_str)
        return False
    
def get_one_timeout_job():    
    """
    called in the RequestJob class
    just to check the JOB_CURRENT_WORKING_JOBS,
    if there is any job that is timeout already, and it is not an urgent job,
    then return that job.  (if it is an urgent job, then ***)
    """
    #@todo: 
    pass
    
    
def get_longest_queue():
    """
    will check the job queues, and see which queue is the longest,
    then return the queue name of the longest queue
    """
    #===========================================================================
    # @attention: REMEMBER to add the new job_type here if has new job_types!!
    #===========================================================================
    job_types = {job_const.JOB_TYPE_FOLLOW, job_const.JOB_TYPE_BI_FOLLOW_ID, job_const.JOB_TYPE_USER_WEIBO}
    queue_len_dict = {}
    for job_type in job_types:
        queue_len_dict[job_type] = master_redis.llen(job_type)
    return max(queue_len_dict.iteritems(), key=operator.itemgetter(1))[0]

def is_urgent_job_in_queue(queue_job):
    """
    check if the queue_job is in the job_urgent_queue or the job_current_working_jobs
    @param queue_job:  a job dict object
    @return:  job_position, which indicates where the job is (job_urgent_queue or job_current_working_jobs)
    """
    job_position = None
    queue_job_json_str = json.dumps(queue_job)
    for job_str in master_redis.lrange(job_const.JOB_URGENT_QUEUE, 0, -1):
        if job_str == queue_job_json_str:
            job_position = job_const.JOB_URGENT_QUEUE
    if not job_position:
        #@todo: 
        pass
    else:
        return job_position

def master_timeout(signum, frame):
    raise TimeOutError, "Timeout"

def process_master_return(queue_job):
    """
    """
    if queue_job:
        if queue_job['job_source'] == job_const.JOB_SOURCE_REALTIME_PRODUCER:
            #===================================================================
            # # This is an urgent job, so...
            # # need to wait until the job is done...
            #===================================================================
            signal.signal(signal.SIGALRM, master_timeout)
            timeout_seconds = 50
            signal.alarm(timeout_seconds)
            try:
                while is_urgent_job_in_queue(queue_job):
                    pass
            except TimeOutError:
                #===============================================================
                # if timeout, return False directly..
                #===============================================================
                return False
        else:
            return True
    else:
        # if queue_job is False, return False directly
        return False
    
class TimeOutError(Exception):
    def __init__(self, current_status):
        self.current_status = current_status
        
    def __str__(self):
        return repr(self.current_status)

#to render the index page
class Index:
    def GET(self):
        return 'index'

    def POST(self): 
        return


"""
#===============================================================================
# In Redis:
#    there are several job queues, each job queue is a list, 
#          and each element in the list would be a json object, 
#            because Redis only stores strings as values.
#        currently, the queues are as follows:
#           job_const.JOB_TYPE_FOLLOW: ***
#           job_const.JOB_TYPE_BI_FOLLOW_ID: ***
#      #======================================================================
#      # current job_types: please refer to job_const.py
#      #======================================================================
#===============================================================================
"""
class JobQueue:
    """
    interface with both job_producer and website(Realtime Producer) 
    """
    def GET(self):
        follow_queue_length = master_redis.llen(job_const.JOB_TYPE_FOLLOW)
        bi_follow_id_queue_length = master_redis.llen(job_const.JOB_TYPE_BI_FOLLOW_ID)
        job_queue_length = {job_const.JOB_TYPE_FOLLOW: follow_queue_length, \
                            job_const.JOB_TYPE_BI_FOLLOW_ID: bi_follow_id_queue_length}
        web.header('Content-Type', 'application/json')
        data_string = json.dumps(job_queue_length)
        return data_string

    def POST(self): 
        return


class Follow:
    """
    interface with both job_producer and website(Realtime Producer) 
    """
    def GET(self):
        return 'get follow'

    def POST(self): 
        post_job_data = web.data()
        post_job_json = json.loads(post_job_data)
        queue_job = push_job_queue(post_job_json)
        return process_master_return(queue_job)


class BiFollowId:
    """
    interface with both job_producer and website(Realtime Producer) 
    """
    def GET(self):
        return 'get BiFollowId'

    def POST(self): 
        post_job_data = web.data()
        post_job_json = json.loads(post_job_data)
        queue_job = push_job_queue(post_job_json)
        return process_master_return(queue_job)


class UserWeibo:
    """
    interface with both job_producer and website(Realtime Producer) 
    """
    def GET(self):
        return 'get user weibo'

    def POST(self): 
        post_job_data = web.data()
        post_job_json = json.loads(post_job_data)
        queue_job = push_job_queue(post_job_json)
        return process_master_return(queue_job)
    

class CheckVersion:
    """
    interface with the Crawlers
    """
    def GET(self):
        return 'check version'

    def POST(self): 
        post_data = web.data()
        version_accept = True
        try:
            post_json = json.loads(post_data)
            if int(post_json['version']) < int(crawler_version):
                version_accept = False
        except:
            error_str = ('Error of CheckVersion %s' % (sys.exc_info()[0]))
            logger.error(error_str)
            version_accept = False
        else:
            pass
        finally:
            check_version_return = {'version_accept': version_accept}
            web.header('Content-Type', 'application/json')
            to_return_str = json.dumps(check_version_return)
            return to_return_str
    

class RequestJob:
    """
    interface with the Crawlers
    """
    def GET(self):
        return 'request job'

    def POST(self): 
        #=======================================================================
        # REMEMBER that every time the crawler sends request here,
        # record the crawler name!
        # and store the crawler's status (both name and what he is doing)
        #=======================================================================
        crawler_data = web.data()
        crawler_json = json.loads(crawler_data)
        crawler_name = crawler_json['crawler_name']
        job_to_crawler = {'crawler_name': crawler_name}
        urgent_queue_length = master_redis.llen(job_const.JOB_URGENT_QUEUE)
        # Definitely, should check the urgent job queue first!
        if urgent_queue_length:
            # if there are any urgent jobs, deal with it first..#
            urgent_job = master_redis.lpop(job_const.JOB_URGENT_QUEUE)
            urgent_job_json = json.loads(urgent_job)
            job_to_crawler.update({'job_json': urgent_job_json})
        else:
            longest_queue_name = get_longest_queue()
            job = master_redis.lpop(longest_queue_name)
            job_json = json.loads(job)
            job_to_crawler.update({'job_json': job_json})
            pass
        job_start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job_to_crawler.update({job_const.JOB_START_TIME: job_start_time})
        job_json_crawler = json.dumps(job_to_crawler)
        master_redis.sadd(job_const.JOB_CURRENT_WORKING_JOBS, job_json_crawler)
        # @todo: store the crawler's status (both name and what he is doing)
        return job_json_crawler


class DeliverJob:
    """
    interface with the Crawlers
    """
    def GET(self):
        return 'deliver job'

    def POST(self): 
        #=======================================================================
        # REMEMBER that every time the crawler delivers a job here,
        # record the crawler name!
        # and store the crawler's status (both name and what he is doing)
        # Store the job into DB, and then remove the job from the JOB_CURRENT_WORKING_JOBS
        #=======================================================================
        crawler_data = web.data()
        crawler_json = json.loads(crawler_data)
        crawler_name = crawler_json['crawler_name']
        result = True
        if crawler_json['job_type'] == job_const.JOB_TYPE_BI_FOLLOW_ID:
            logger.info(crawler_json['ids'])
            pass
        elif crawler_json['job_type'] == job_const.JOB_TYPE_FOLLOW:
            pass
        elif crawler_json['job_type'] == job_const.JOB_TYPE_USER_WEIBO:
            pass
        # @todo: store the crawler's status (both name and what he is doing)
        return result
    
    
class Others:
    def GET(self,other): 
        if other == 'redis':
            #===================================================================
            #  below is actually to clear the queue in redis...
            #===================================================================
            while master_redis.rpop(job_const.JOB_TYPE_FOLLOW):
                pass
            while master_redis.lpop(job_const.JOB_TYPE_BI_FOLLOW_ID):
                pass
            while master_redis.lpop(job_const.JOB_URGENT_QUEUE):
                pass
            logger.info('has cleared the job queues in Redis..')
            return master_redis.rpop(job_const.JOB_TYPE_FOLLOW)
        elif other == 'orm':
            #===================================================================
            #  just to test the orm...
            #===================================================================
            user_id = '2159515'
            return process_db.has_stored_user_by_uid(user_id)
            pass
        else:
            return other

    def POST(self):
        return
 

