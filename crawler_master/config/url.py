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
     '/statuses_show/',         pre_fix + 'master.StatusesShow', 
     #==========================================================================
     #==========================================================================
     #@attention: below is the interface with crawlers
     '/check_version/',          pre_fix + 'master.CheckVersion', 
     '/request_job/',            pre_fix + 'master.RequestJob', 
     '/deliver_job/',            pre_fix + 'master.DeliverJob', 
     #==========================================================================
     '/monitor',              pre_fix + 'master.MonitorPage', 
     '/(.*)',                 pre_fix + 'master.Others',
) 
