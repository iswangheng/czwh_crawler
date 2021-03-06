#!/usr/bin/env python
# coding: utf-8
import web
import redis
import logging
import os
from logging import handlers

# HERE indicates the current crawler version
# @attention: REMEMBER to update the version whenever you have updated the Crawler
crawler_version = 1.0

db = web.database(dbn='mysql', db='sina_weibo', user='crawler_master', pw='crawler_master')
 
rootdir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
templates_dir = os.path.join(rootdir,'templates/')
render = web.template.render(templates_dir)

web.config.debug = True

config = web.storage(
    email='iswangheng@gmail.com',
    site_name = 'Crawler_Master',
    site_desc = '',
    #Below the the UTC +8 timezone, in the sub app may need to change according to the users timezone!!!! 
    utc_offset = 8, 
)

master_redis = redis.StrictRedis(host='csz908.cse.ust.hk', \
                                 port=6379, db='crawler_master')
"""
will set up the logger for the crawler_master
"""
logger = logging.getLogger('crawler_master')
hdlr = logging.handlers.RotatingFileHandler(filename='./swarm_logs/crawler_master.log', maxBytes=20480000,
                                                backupCount = 10)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


web.template.Template.globals['config'] = config
web.template.Template.globals['render'] = render
