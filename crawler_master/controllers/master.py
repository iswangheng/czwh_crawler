#!usr/bin/env python
# coding: utf-8

import web
import json
import sys
import math
import shlex
import logging
import process_db
import time
import redis
from time import gmtime, strftime
from config import settings

logger = logging.getLogger("crawler_master")
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
            follow_queue_job = {'job_type': job_json['job_type']}
            follow_queue_job.update({'user_id': user_id})
            follow_queue_job_json = json.dumps(follow_queue_job)
            #===================================================================
            # job_souce has two kinds of values: 0 and 1
            #      '0'   <----->   job_producer
            #      '1'   <----->   realtime_producer 
            #      if job_source == '1', means that this is an urgent job,
            #        so we need the crawler to process this job ASAP,
            #        thus the redis should left push the job into the job_queue
            #===================================================================
            if job_json['job_source'] == '0':
                master_redis.rpush(job_json['job_type'], follow_queue_job_json)
            else:
                master_redis.lpush(job_json['job_type'], follow_queue_job_json)
        return True
    except:
        return False
    

#to render the index page
class Index:
    def GET(self):
        return 'index'

    def POST(self): 
        return


class Follow:
    def GET(self):
        return 'get follow'

    def POST(self): 
        post_job_data = web.data()
        post_job_json = json.loads(post_job_data)
        return push_job_queue(post_job_json)

class BiFollowId:
    def GET(self):
        return 'get BiFollowId'

    def POST(self): 
        post_job_data = web.data()
        post_job_json = json.loads(post_job_data)
        return push_job_queue(post_job_json)

"""
#===============================================================================
# In Redis:
#    there are several job queues, each job queue is a list, 
#          and each element in the list would be a json object, 
#            because Redis only stores strings as values.
#        currently, the queues are as follows:
#           '1': ***
#           '2': ***
#      #======================================================================
#      # current job_types:
#      #    '1'   <------>   follow
#      #    '2'   <------>   bi_follow_id
#      #======================================================================
#===============================================================================
"""
class JobQueue:
    def GET(self):
        follow_queue_length = master_redis.llen('1')
        bi_follow_id_queue_length = master_redis.llen('2')
        job_queue_length = {'1': follow_queue_length, \
                            '2': bi_follow_id_queue_length}
        web.header('Content-Type', 'application/json')
        data_string = json.dumps(job_queue_length)
        return data_string

    def POST(self): 
        return


class Others:
    def GET(self,other): 
        if other == 'redis':
            #===================================================================
            #  below is actually to clear the queue in redis...
            #===================================================================
            while master_redis.rpop('1'):
                master_redis.rpop('2')
            logger.info('has cleared the job queues in redis..')
            return master_redis.rpop('2')
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
 

