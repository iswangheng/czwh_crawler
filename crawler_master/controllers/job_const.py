'''
Created on 7 Nov, 2012

@author: swarm
'''
JOB_ID = "job_id"
JOB_CURRENT_WORKING_JOBS = "job_current_working_jobs"
JOB_CURRENT_WORKING_CRAWLER = "job_current_working_crawler"
JOB_START_TIME = "job_start_time"

JOB_SOURCE_JOB_PRODUCER = "job_producer"
JOB_SOURCE_REALTIME_PRODUCER = "realtime_producer"

JOB_TYPE_FOLLOW = "follow"
JOB_TYPE_BI_FOLLOW_ID= "bi_follow_id"
JOB_TYPE_USER_WEIBO = "user_weibo"
JOB_TYPE_STATUSES_SHOW = "statuses_show"

JOB_FOLLOW_MAX_NUM = 2000
JOB_BI_FOLLOW_MAX_NUM = 2000
JOB_USER_WEIBO_MAX_NUM = 2000


#===============================================================================
# there are several job queues in crawler_master
# common job queues' names are:
# from the JOB_TYPE_***    
# ONE SPECIAL CASE is the JOB_URGENT_QUEUE, this is an urgent queue,
#  thus the master would give the HIGHEST priority to this urgent queue
#===============================================================================
JOB_URGENT_QUEUE = 'urgent_jobs'