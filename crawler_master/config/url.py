#!/usr/bin/env python
# coding: utf-8

pre_fix = 'controllers.'

urls = (
     '/',                     pre_fix + 'master.Index', 
     '/job_queue/',              pre_fix + 'master.JobQueue', 
     '/follow/',              pre_fix + 'master.Follow', 
     '/bi_follow_id/',          pre_fix + 'master.BiFollowId', 
     '/(.*)',                 pre_fix + 'master.Others',
) 
