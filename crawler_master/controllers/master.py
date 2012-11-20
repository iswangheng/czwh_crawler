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
from datetime import timedelta
from config import settings
from collections import defaultdict

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
    @return: False or queue_job (a dict object)
    """
    try:
        for user_id in job_json['user_id_list']:
            job_id = master_redis.incr(job_const.JOB_ID)
            queue_job = {'job_id': job_id}
            queue_job.update({'job_type': job_json['job_type']})
            queue_job.update({'job_source': job_json['job_source']})
            queue_job.update({'user_id': user_id})
            queue_job.update({'max_num': job_json['max_num']})
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

def push_status_job_queue(job_json):
    """
    this is to push the status job_json into the status job_queue 
                                      means:  job_const.JOB_TYPE_STATUSES_SHOW
    @param job_json: each job_json contains the job_type and a statuses_id_list.. 
    @return: False or queue_job (a dict object)
    """
    try:
        job_id = master_redis.incr(job_const.JOB_ID)
        job_json.update({'job_id': job_id})
        queue_job_json_str = json.dumps(job_json)
        if job_json['job_source'] == job_const.JOB_SOURCE_REALTIME_PRODUCER:
            master_redis.lpush(job_const.JOB_URGENT_QUEUE, queue_job_json_str)
        else:
            logger.info('will rpush the status_show job into the list queue')
            master_redis.rpush(job_json['job_type'], queue_job_json_str)
        return job_json
    except:
        error_str = ('Error of push_status_job_queue %s' % (sys.exc_info()[0]))
        logger.error(error_str)
        return False
    
def remove_return_timeout_jobs():    
    """
    called in the RequestJob class
    just to check the JOB_CURRENT_WORKING_CRAWLER, JOB_CURRENT_WORKING_JOBS,
    and remove the jobs that are timeout already from the current_working_*,
    and in the end, return those timeout_jobs (for further use: say put them again in the job_queues)
    """
    timeout_seconds = 4*60
    job_json_list = []
    try:
        length_current_working = master_redis.llen(job_const.JOB_CURRENT_WORKING_CRAWLER)
        i = 0 
        if length_current_working:
            while 1:
                job_to_crawler_json_str = master_redis.lindex(job_const.JOB_CURRENT_WORKING_CRAWLER, i)
                job_to_crawler = json.loads(job_to_crawler_json_str)
                time_now = datetime.datetime.now()
                job_start_time = datetime.datetime.strptime(job_to_crawler[job_const.JOB_START_TIME], "%Y-%m-%d %H:%M:%S")
                if (time_now - job_start_time) > timedelta(seconds=timeout_seconds):
                    master_redis.lrem(job_const.JOB_CURRENT_WORKING_CRAWLER, count=0, value=job_to_crawler_json_str)
                    master_redis.srem(job_const.JOB_CURRENT_WORKING_JOBS, json.dumps(job_to_crawler['job_json']))
                    job_json_list.append(job_to_crawler['job_json'])
                    length_current_working -= 1
                    i -= 1
                if i+1 == length_current_working:
                    break
                i += 1
    except:
        logger.error('remove_timeout_jobs error')
        logger.error('%s %s ' % (sys.exc_info()[0], sys.exc_info()[1]))
        logger.info('maybe the current_working is empty already')
    finally:
        return job_json_list
        
def remove_job_from_current_working(crawler_json):
    """
    will remove the completed job from those current_working:
             job_const.JOB_CURRENT_WORKING_CRAWLER,
          and job_const.JOB_CURRENT_WORKING.JOBS
    """
    try:
        length_current_working = master_redis.llen(job_const.JOB_CURRENT_WORKING_CRAWLER)
        i = 0 
        while 1:
            job_to_crawler_json_str = master_redis.lindex(job_const.JOB_CURRENT_WORKING_CRAWLER, i)
            job_to_crawler = json.loads(job_to_crawler_json_str)
            if job_to_crawler['job_json'] == crawler_json['job_json']: 
                master_redis.lrem(job_const.JOB_CURRENT_WORKING_CRAWLER, count=0, value=job_to_crawler_json_str)
                master_redis.srem(job_const.JOB_CURRENT_WORKING_JOBS, json.dumps(crawler_json['job_json']))
                length_current_working -= 1
                i -= 1
                break
            if i+1 == length_current_working:
                break
    except:
        logger.error('remove_job_from_current_working error')
        logger.error('%s %s ' % (sys.exc_info()[0], sys.exc_info()[1]))
    pass

def get_longest_queue():
    """
    will check the job queues, and see which queue is the longest,
    then return the queue name of the longest queue
    """
    #===========================================================================
    # @attention: REMEMBER to add the new job_type here if there are new job_types!!
    #===========================================================================
    job_types = [job_const.JOB_TYPE_FOLLOW, job_const.JOB_TYPE_BI_FOLLOW_ID, \
                 job_const.JOB_TYPE_USER_WEIBO, job_const.JOB_TYPE_STATUSES_SHOW]
    return max(job_types, key=lambda job_type: master_redis.llen(job_type))

def urgent_job_position(queue_job):
    """
    check where the queue_job is: is it in the job_urgent_queue or the job_current_working_jobs or neither
    @param queue_job:  a job dict object
    @return:  job_position, which indicates where the job is (job_urgent_queue or job_current_working_jobs)
              job_position can be either one of them {job_urgent_queue, job_current_working_jobs, None}
    """
    job_position = None
    queue_job_json_str = json.dumps(queue_job)
    if any(job_str == queue_job_json_str for job_str in master_redis.lrange(job_const.JOB_URGENT_QUEUE, 0, -1)):
        job_position = job_const.JOB_URGENT_QUEUE
    #===========================================================================
    #=== equivalent to the code below:
    # for job_str in master_redis.lrange(job_const.JOB_URGENT_QUEUE, 0, -1):
    #    if job_str == queue_job_json_str:
    #        job_position = job_const.JOB_URGENT_QUEUE
    #        break
    #===========================================================================
    if not job_position:
        # if not in job_urgent_queue, then check the job_current_working_jobs
        if master_redis.sismember(job_const.JOB_CURRENT_WORKING_JOBS, queue_job_json_str):
            job_position = job_const.JOB_CURRENT_WORKING_JOBS
    else:
        # if it is in job_urgent_queue, then return the position directly
        return job_position
    return job_position

def master_timeout(signum, frame):
    raise TimeOutError, "Timeout"

def process_master_return(queue_job):
    """
    used in the Class (interface with the job_producer and realtime_job_producer)
    will determine what to return the job_producer or real time job_producer
    if it is an urgent job (comes from the real time job_producer), then should return until the job is done or timeout.
    if is not urgent, then can return directly
    """
    if queue_job:
        if queue_job['job_source'] == job_const.JOB_SOURCE_REALTIME_PRODUCER:
            #===================================================================
            # # This is an urgent job, so...
            # # need to wait until the job is done...
            #===================================================================
            signal.signal(signal.SIGALRM, master_timeout)
            timeout_seconds = 150
            signal.alarm(timeout_seconds)
            job_position = True
            try:
                # wait until the job is done or timeout
                while job_position:
                    job_position = urgent_job_position(queue_job)
                    pass
            except TimeOutError:
                #===============================================================
                # if timeout, remove the job from the current_working_set and return False 
                #===============================================================
                queue_job_json_str = json.dumps(queue_job)
                try:
                    if job_position == job_const.JOB_CURRENT_WORKING_JOBS:
                        master_redis.srem(job_position, queue_job_json_str)
                except:
                    logger.error('master_redis set remove error.. maybe it has been removed already')
                return False
            except:
                logger.error('urgent_job_position(queue_job) error, may be the redis')
        else:
            #===================================================================
            # # This is NOT an urgent job, 
            # # Then can return True directly, no matter what happens to the job
            #===================================================================
            return True
    else:
        # if queue_job is False, 
        #  means the push_job_queue() returns False, 
        # thus
        #  return False directly
        return False
    
class TimeOutError(Exception):
    def __init__(self, current_status):
        self.current_status = current_status
        
    def __str__(self):
        return repr(self.current_status)



def fill_in_working_crawlers():
    #=======================================================================
    # working_crawler_dict = {"crawler_name": "test", \
    #                        "crawler_ip": "123", \
    #                        "working_on": "test", \
    #                        "start_time": "time"}
    #=======================================================================
    working_crawlers = []
    for current_crawler_str in master_redis.lrange(job_const.JOB_CURRENT_WORKING_CRAWLER, 0, -1):
        current_crawler = json.loads(current_crawler_str)
        working_crawler_dict = defaultdict(str)
        working_crawler_dict['crawler_name'] = current_crawler['crawler_name']
        working_crawler_dict['crawler_ip'] = current_crawler['crawler_ip']
        working_crawler_dict['start_time'] = current_crawler[job_const.JOB_START_TIME]
        working_on_str = "JobID: %s | JobType: %s | JobSource: %s" % \
                        (current_crawler['job_json']['job_id'], \
                        current_crawler['job_json']['job_type'], \
                        current_crawler['job_json']['job_source'], \
                         )
        working_crawler_dict['working_on'] = working_on_str
        working_crawlers.append(working_crawler_dict)
    return working_crawlers
        
def fill_in_job_queues():
    #=======================================================================
    # job_queue_dict = {"job_type": "job_type", \
    #                  "job_number": "123" }
    #=======================================================================
    job_types = [job_const.JOB_TYPE_FOLLOW, job_const.JOB_TYPE_BI_FOLLOW_ID, \
                 job_const.JOB_TYPE_USER_WEIBO, job_const.JOB_TYPE_STATUSES_SHOW]
    job_queues = []
    for job_type in job_types:
        job_queue_dict = defaultdict(str)
        job_queue_dict['job_type'] = job_type
        job_numbers = master_redis.llen(job_type)
        job_queue_dict['job_number'] = job_numbers
        job_queues.append(job_queue_dict)
    return job_queues
        
        
        
# to render the index page
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
#           job_const.JOB_TYPE_USER_WEIBO: ***
#           job_const.JOB_TYPE_STATUSES_SHOW: ***
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
        user_weibo_queue_length = master_redis.llen(job_const.JOB_TYPE_USER_WEIBO)
        statuses_show_queue_length = master_redis.llen(job_const.JOB_TYPE_STATUSES_SHOW)
        job_queue_length = {job_const.JOB_TYPE_FOLLOW: follow_queue_length, \
                            job_const.JOB_TYPE_BI_FOLLOW_ID: bi_follow_id_queue_length, \
                            job_const.JOB_TYPE_USER_WEIBO: user_weibo_queue_length, \
                            job_const.JOB_TYPE_STATUSES_SHOW: statuses_show_queue_length}
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


class StatusesShow:
    """
    interface with both job_producer and website(Realtime Producer) 
    """
    def GET(self):
        return 'get statuses by status_id'

    def POST(self): 
        """
        here is a special job_type, added by Dr. Lei CHEN,
        so we need to deal with this job individually
        """
        post_job_data = web.data()
        post_job_json = json.loads(post_job_data)
        queue_job = push_status_job_queue(post_job_json)
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
        # remove those jobs that are timeout already
        # and then put the timeout jobs in the job queues again 
        job_json_list = remove_return_timeout_jobs()
        for job_json in job_json_list:
            master_redis.rpush(job_const.JOB_URGENT_QUEUE, json.dumps(job_json))
        #=======================================================================
        # REMEMBER that every time the crawler sends request here,
        # record the crawler name!
        # and store the crawler's status (both name and what he is doing)
        #=======================================================================
        crawler_data = web.data()
        crawler_json = json.loads(crawler_data)
        crawler_name = crawler_json['crawler_name']
        crawler_ip = web.ctx.ip
        job_to_crawler = {'crawler_name': crawler_name}
        job_to_crawler.update({'crawler_ip': crawler_ip})
        urgent_queue_length = master_redis.llen(job_const.JOB_URGENT_QUEUE)
        job_json = None
        # Definitely, should check the urgent job queue first!
        if urgent_queue_length:
            # if there are any urgent jobs, deal with it first..#
            logger.info('urgent_queue_length is : %s' % (urgent_queue_length))
            urgent_job = master_redis.lpop(job_const.JOB_URGENT_QUEUE)
            job_json = json.loads(urgent_job)
            job_to_crawler.update({'job_json': job_json})
        else:
            longest_queue_name = get_longest_queue()
            logger.info('longest_queue_name is: %s' % (longest_queue_name))
            job = master_redis.lpop(longest_queue_name)
            if not job:
                job_json = None
            else:
                job_json = json.loads(job)
            job_to_crawler.update({'job_json': job_json})
        job_start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job_to_crawler.update({job_const.JOB_START_TIME: job_start_time})
        job_to_crawler_json_str = json.dumps(job_to_crawler)
        if job_json:
            job_json_str = json.dumps(job_to_crawler['job_json'])
            # after popping the job from the queue, add it into the current_working_jobs set
            master_redis.sadd(job_const.JOB_CURRENT_WORKING_JOBS, job_json_str)
            # store the crawler's status (both name and what he is doing)
            master_redis.rpush(job_const.JOB_CURRENT_WORKING_CRAWLER, job_to_crawler_json_str)
        return job_to_crawler_json_str


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
        # Store the job into DB, 
        #    and then remove the job from the JOB_CURRENT_WORKING_JOBS
        #    also remove the job from the JOB_CURRENT_WORKING_CRAWLER 
        #=======================================================================
        crawler_data = web.data()
        crawler_json = json.loads(crawler_data)
        crawler_name = crawler_json['crawler_name']
        result = True
        if crawler_json['job_type'] == job_const.JOB_TYPE_BI_FOLLOW_ID:
            process_db.handle_bi_follow_id(crawler_json)
            remove_job_from_current_working(crawler_json)
        elif crawler_json['job_type'] == job_const.JOB_TYPE_FOLLOW:
            process_db.handle_follow(crawler_json)
            remove_job_from_current_working(crawler_json)
        elif crawler_json['job_type'] == job_const.JOB_TYPE_USER_WEIBO:
            process_db.handle_user_weibo(crawler_json)
            remove_job_from_current_working(crawler_json)
        elif crawler_json['job_type'] == job_const.JOB_TYPE_STATUSES_SHOW:
            logger.info("Received a statuses_show job from crawler, will handle it and remove")
            process_db.handle_statuses_show(crawler_json)
            remove_job_from_current_working(crawler_json)
        # @todo: store the crawler's status (both name and what he is doing)
        return result
    

class MonitorPage:
    """
    just to show the current working crawlers and also the job queues status
    """
    def GET(self):
        working_crawlers = fill_in_working_crawlers()
        job_queues = fill_in_job_queues() 
        return render.monitor(working_crawlers, job_queues)

    def POST(self): 
        return "monitor page"

    
class Others:
    def GET(self, other): 
        if other == 'redis':
            #===================================================================
            #  below is actually to clear the queue in redis...
            #===================================================================
            while master_redis.rpop(job_const.JOB_TYPE_FOLLOW):
                pass
            while master_redis.lpop(job_const.JOB_TYPE_BI_FOLLOW_ID):
                pass
            while master_redis.lpop(job_const.JOB_TYPE_USER_WEIBO):
                pass
            while master_redis.lpop(job_const.JOB_TYPE_STATUSES_SHOW):
                pass
            while master_redis.lpop(job_const.JOB_URGENT_QUEUE):
                pass
            while master_redis.lpop(job_const.JOB_CURRENT_WORKING_CRAWLER):
                pass
            while master_redis.spop(job_const.JOB_CURRENT_WORKING_JOBS):
                pass
            logger.info('has cleared the job queues in Redis..')
            return master_redis.rpop(job_const.JOB_TYPE_FOLLOW)
        elif other == 'orm':
            #===================================================================
            #  just to test the orm...
            #===================================================================
            user_id = '2159515'
            return process_db.has_stored_user_by_uid(user_id)
        elif other == 'ip':
            #===================================================================
            #  just to test getting remote IP Address
            #===================================================================
            ip_address = web.ctx.ip
            return ("IP Address: %s" % (ip_address))
        else:
            return other

    def POST(self):
        return
 

#===============================================================================
#   Just for reference
#  #What's in Redis?
    #===================================================================
    # list:  job_const.JOB_TYPE_FOLLOW
    # list:  job_const.JOB_TYPE_BI_FOLLOW_ID
    # list:  job_const.JOB_TYPE_USER_WEIBO
    # list:  job_const.JOB_TYPE_STATUSES_SHOW
    # list:  job_const.JOB_URGENT_QUEUE
    # list:  job_const.JOB_CURRENT_WORKING_CRAWLER
    # set:   job_const.JOB_CURRENT_WORKING_JOBS
    #===================================================================
#===============================================================================
