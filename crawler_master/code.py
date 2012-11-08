#!/usr/bin/env python
# coding: utf-8
import sys, os
abspath = os.path.dirname(__file__)
sys.path.append(abspath)
os.chdir(abspath)
from config.url import urls
from config import settings
import web


db = settings.db
web.config.debug = True
app = web.application(urls, globals())
store = web.session.DBStore(db, 'sessions')


if web.ctx.get('session') is None:
    session = web.session.Session(app, store, {'test':'',
                                  'test_one':'',
                                  'test_two':''})

    web.ctx.session = session
else:
    session = web.ctx.session


def session_hook():
    web.ctx.session = session


app.add_processor(web.loadhook(session_hook))  
application = app.wsgifunc()

