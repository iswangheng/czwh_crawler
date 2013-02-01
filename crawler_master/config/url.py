#!/usr/bin/env python
# coding: utf-8

pre_fix = 'controllers.'

urls = (
     '/',                     pre_fix + 'master.Index', 
     #==========================================================================
     #@attention: below is the interface with both job_producer and website(Realtime Producer) 
     '/job_queue/',           pre_fix + 'master.JobQueue', 
     '/follow/',              pre_fix + 'master.Follow', 
     '/bi_follow_id/',        pre_fix + 'master.BiFollowId', 
     '/user_weibo/',          pre_fix + 'master.UserWeibo', 
     '/statuses_show/',       pre_fix + 'master.StatusesShow', 
     '/user_show/',           pre_fix + 'master.UserShow', 
     #==========================================================================
     #==========================================================================
     #@attention: below is the interface with crawlers
     '/check_version/',          pre_fix + 'master.CheckVersion', 
     '/request_job/',            pre_fix + 'master.RequestJob', 
     '/deliver_job/',            pre_fix + 'master.DeliverJob', 
     #==========================================================================
     #@attention: below is the interface with searcher (search weibo with keywords)
     '/deliver_search_results/', pre_fix + 'master.SearchResults', 
     #==========================================================================
     #==========================================================================
     #@attention: below is to copy the db's user_id/status_id/follow/bi_follow_id 
     #                 into Redis (just to build cache for better performance)
     '/db_to_redis/(.*)',        pre_fix + 'master.DatabaseToRedis', 
     #==========================================================================
     '/monitor',              pre_fix + 'master.MonitorPage', 
     '/(.*)',                 pre_fix + 'master.Others',
) 
