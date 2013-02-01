#!/usr/bin/env python

'''
Created on 9 Nov, 2012

@author: swarm
'''
import logging
from logging import handlers
from ConfigParser import ConfigParser
import job_const
import os
import weibo
import time
import urllib2
import sys
import math
import json
#import simplejson


class Crawler(object):
    '''
    the Crawler Class
    '''
    def __init__(self):
        #=======================================================================
        # @attention: This variable version should be fixed at each version
        self.version = 1.0    
        # @attention: Remember to change it whenever you release a new version
        #=======================================================================
        self.logger = self.set_logger()
        self.config = self.set_config()
        self.token = None
        self.client = None
        self.error_handler = ErrorHandler(self.logger)
        self.crawler_name = self.config.get('crawler', 'crawler_name')
        self.crawler_master_url = self.config.get('crawler', 'master_url')
        self.token_server_url = self.config.get('crawler', 'token_server_url')
        self.sleep_min = self.config.get('crawler', 'sleep_min')
        pass 
    
    def set_logger(self):
        """
        will set up the logger for Crawler
        """
        logger = logging.getLogger("Crawler")
        hdlr = logging.handlers.RotatingFileHandler(filename='crawler.log', maxBytes=20480000, \
                                                        backupCount = 10)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.INFO)
        return logger

    def set_config(self):
        """ will read crawler_config.ini and then return the config for further use
        """
        filename = os.path.join('.', 'crawler_config.ini')
        config = ConfigParser()
        config.read(filename)
        return config
    
    def check_version(self):
        """
        will ask the crawler_master to check whether this version is okay or not
        """
        check_version_url = self.crawler_master_url + 'check_version/'
        version_accept = False
        version_dict = {'version': self.version}
        version_json = json.dumps(version_dict)
        req = urllib2.Request(url=check_version_url, \
                              data=version_json, \
                              headers={'Content-Type': 'application/json'})
        r = urllib2.urlopen(req)
        try:
#            version_res_json = simplejson.load(r)
            version_res_json = json.load(r)
            version_accept = version_res_json['version_accept']
        except:
            self.logger.error("version_accept load json error..")
        finally:
            return version_accept
    
    def get_token(self):
        """
        will ask the token_server to get one token to start crawling
        """
        access_token = None
        token_url = self.token_server_url + 'get'
        req = urllib2.Request(url=token_url)
        r = urllib2.urlopen(req)
        print "will get token from: %s" % token_url
        try:
#            res_json = simplejson.load(r)
            res_json = json.load(r)
            access_token = res_json['access_token']
            print "access_token: %s" % access_token
        except:
            access_token = None
            err_str = "access_token json loading error"
            print err_str
            self.logger.error(err_str)
        finally:
            return access_token

    def limit_expire_token(self, limit_or_expire, access_token):
        """
        will tell the token_server the access_token has been out of limit or expired
        """
        token_url = "%s%s?access_token=%s" % (self.token_server_url, limit_or_expire, access_token)
        req = urllib2.Request(url=token_url)
        r = urllib2.urlopen(req)
        return 
    
    def request_job(self):
        """
        will ask the crawler_master to get one job to start crawling
        """
        request_job_url = self.crawler_master_url + 'request_job/'
        request_dict = {'crawler_name': self.crawler_name}
        request_json= json.dumps(request_dict)
        res_json = None
        req = urllib2.Request(url=request_job_url, \
                              data=request_json, \
                              headers={'Content-Type': 'application/json'})
        r = urllib2.urlopen(req)
        try:
#            res_json = simplejson.load(r)
            res_json = json.load(r)
        except:
            error_str = ('Error of request_job json.load %s' % (sys.exc_info()[0]))
            self.logger.error(error_str)
            res_json = None
        finally:
            return res_json
    
    def deliver_job(self, to_deliver):
        """
        will deliver the completed job to crawler_master
        @param to_deliver: a dict object 
        """
        deliver_job_url = self.crawler_master_url + 'deliver_job/'
        to_deliver.update({'crawler_name': self.crawler_name})
        to_deliver_json = json.dumps(to_deliver)
        req = urllib2.Request(url=deliver_job_url, \
                              data=to_deliver_json, \
                              headers={'Content-Type': 'application/json'})
        r = urllib2.urlopen(req)
        if r.read() == "True":
            print "Successfully delivered this job"
        else:
            print "Failed to deliver the job.."
        return 
    
    def crawl_follow(self, user_id):
        """
        would start crawling the user_id's follow relation through Sina Weibo API 
        @param user_id:  
        @return: follow_json_list, a list containing the json returned by sina weibo
        """
        print "will crawl %s follow " % user_id
        follow_json_list = []
        next_cursor = 0
        while 1:
            try:
                print "crawl_follow----> next_cursor----->: %s" % next_cursor
                follow_response = self.client.friendships__friends(uid=user_id, count=200, cursor=next_cursor)
                follow_json = json.loads(follow_response)
            except weibo.APIError, api_error:
                raise self.error_handler.WeiboAPIError(api_error.error_code, api_error.error, \
                                                       api_error.request, self)
            else:
                try:
                    next_cursor = follow_json['next_cursor']
                except:
                    self.logger.error("%s has no next_cursor while crawling follow" % (user_id) )
                    break
                else:
                    follow_json_list.append(follow_json)
                    if not next_cursor:
                        break
                    if next_cursor > 2000:
                        break
            sleep_seconds = 1 
            time.sleep(sleep_seconds)
        return follow_json_list 
        
    def crawl_bi_follow_id(self, user_id):
        """
        would start crawling the user_id's bi_follow_id through Sina Weibo API 
        @param user_id:  
        @return: the json returned by sina weibo
        """
        print "will crawl %s bi_follow_id " % user_id
        try:
            friends_ids_response = self.client.friendships__friends__bilateral__ids(uid=user_id, count=2000, page=1)
            friends_ids_json = json.loads(friends_ids_response)
        except weibo.APIError, api_error:
            raise self.error_handler.WeiboAPIError(api_error.error_code, api_error.error, \
                                                   api_error.request, self)
        return friends_ids_json 
        
    def crawl_user_weibo(self, user_id):
        """
        would start crawling the user_id's weibo through Sina Weibo API 
        @param user_id:  
        @return: the statuses list containing the statuses returned by sina weibo
        """
        print "will crawl %s weibo" % user_id
        statuses_list = []
        page = 1
        total_num = 0
        loop_num = 1
        while 1:
            try:
                user_weibo_res = self.client.statuses__user_timeline(
                                    uid=user_id, count=100, page=page)
                user_weibo_json = json.loads(user_weibo_res)
                if len(user_weibo_json['statuses']):
                    statuses_list.extend(user_weibo_json['statuses'])
                else:
                    break     
                # if first time, get the total_num of weibo
                if page == 1:
                    total_num = user_weibo_json['total_number']
                    loop_num = int(math.ceil(total_num/100.0))
                page = page + 1
                if page > loop_num:
                    break
                #===============================================================
                # # @attention: page > * , here is to control how many statuses to get in total
                #===============================================================
                if page > 10:
                    break
            except weibo.APIError, api_error:
                raise self.error_handler.WeiboAPIError(api_error.error_code, api_error.error, \
                                                       api_error.request, self)
            except:
                #"maybe there is no weibo of this user..."
                error_str = ('%s %s' % (sys.exc_info()[0], sys.exc_info()[1]) )
                self.logger.error(error_str)
                print "maybe user_weibo_json has no statuses"
                return statuses_list
        return statuses_list
    
    def crawl_statuses_show(self, statuses_id_list):
        """
        would start crawling the statuses according to the statuses_id_list by Sina Weibo API 
        @param statuses_id_list: a list containing the ids of the statuses to crawl  
        @return: a list containing the jsons returned by sina weibo
        """
        print "will crawl these statuses: %s " % str(statuses_id_list)
        statuses_list = []
        for status_id in statuses_id_list:
            try:
                status_response = self.client.statuses__show(id=status_id)
                status_json = json.loads(status_response)
            except weibo.APIError, api_error:
                if api_error.error_code == 20101:
                    #===========================================================
                    # 20101: the status does not existed any more
                    #===========================================================
                    # if not existed
                    print "this weibo does not exist any longer!!!"
                    status_json = {"id": status_id}
                    status_json.update({"exist": False})
                    statuses_list.append(status_json)
                    pass
                else:
                    raise self.error_handler.WeiboAPIError(api_error.error_code, api_error.error, \
                                                           api_error.request, self)
            else:
                print "Okay, this weibo exists still and will be added into the list!!!"
                status_json.update({"exist": True})
                statuses_list.append(status_json)
            finally:
                time.sleep(1)
        return statuses_list
    
    def crawl_user_show(self, user_id):
        """
        would start crawling the user info according to the user_id by Sina Weibo API 
        @param user_id: 
        @return: sina_weibo_json returned by sina weibo
        """
        print "will crawl %s user info " % str(user_id)
        user_response = None
        try:
            user_response = self.client.users__show(uid=user_id)
            user_response_dict = json.loads(user_response)
        except weibo.APIError, api_error:
            if api_error.error_code == 20003:
                #===========================================================
                # 20003: the User does not existed any more
                #===========================================================
                # if not existed
                print "this User does NOT exist any longer!!!"
                user_response_dict = {"id": user_id}
                user_response_dict.update({"exist": False})
            else:
                raise self.error_handler.WeiboAPIError(api_error.error_code, api_error.error, \
                                                       api_error.request, self)
        else:
            print "Okay, this User %s exists still.." % (user_id)
            user_response_dict.update({"exist": True})
        finally:
            time.sleep(0.1)
        return user_response_dict 
    
    def crawl_by_weibo_api(self, job_json):
        """
        would start crawling through Sina Weibo API based on the info from job_json
        @param job_json:  = {"job_type": **, "user_id":**, ... } 
        @return: the json returned by sina weibo
        """
        to_deliver = {'job_type': job_json['job_type']}
        to_deliver.update({'job_json': job_json})
        if job_json['job_type'] == job_const.JOB_TYPE_BI_FOLLOW_ID:
            if job_json.has_key('user_id'):
                friends_ids_response = self.crawl_bi_follow_id(job_json['user_id'])
                to_deliver.update({'user_id': job_json['user_id']})
                to_deliver.update({'sina_weibo_json':friends_ids_response})
            else:
                raise self.error_handler.JobError('%s has not such key user_id' % job_json['job_json'])
        elif job_json['job_type'] == job_const.JOB_TYPE_FOLLOW:
            if job_json.has_key('user_id'):
                follow_response = self.crawl_follow(job_json['user_id'])
                to_deliver.update({'user_id': job_json['user_id']})
                to_deliver.update({'sina_weibo_json': follow_response})
            else:
                raise self.error_handler.JobError('%s has not such key user_id' % job_json['job_json'])
        elif job_json['job_type'] == job_const.JOB_TYPE_USER_WEIBO:
            if job_json.has_key('user_id'):
                user_weibo_response = self.crawl_user_weibo(job_json['user_id'])
                to_deliver.update({'user_id': job_json['user_id']})
                to_deliver.update({'sina_weibo_json': user_weibo_response})
            else:
                raise self.error_handler.JobError('%s has not such key user_id' % job_json['job_json'])
        elif job_json['job_type'] == job_const.JOB_TYPE_STATUSES_SHOW:
            if job_json.has_key('statuses_id_list'):
                statuses_show_list = self.crawl_statuses_show(job_json['statuses_id_list'])
                to_deliver.update({'sina_weibo_json_list': statuses_show_list})
            else:
                raise self.error_handler.JobError('%s has not such key statuses_id_list' % job_json['job_json'])
        elif job_json['job_type'] == job_const.JOB_TYPE_USER_SHOW:
            if job_json.has_key('user_id'):
                user_response = self.crawl_user_show(job_json['user_id'])
                to_deliver.update({'sina_weibo_json': user_response})
            else:
                raise self.error_handler.JobError('%s has not such key user_id' % job_json['job_json'])
        else:
            pass
        return to_deliver
    
    def send_weibo_log(self, job_json):
        text_str = "Just finished job_id: %s, job_type: %s, job_source: %s" % \
                   (job_json['job_id'], job_json['job_type'], job_json['job_source'])
        print text_str
        #=======================================================================
        # try:
        #    status_update_response = self.client.post.statuses__update(status=text_str)
        # except weibo.APIError, api_error:
        #    raise self.error_handler.WeiboAPIError(api_error.error_code, api_error.error, \
        #                                           api_error.request, self)
        #=======================================================================
        return True
    
    def start(self):
        """
        the crawler would start AFTER the version has been checked  
            In this part: the crawler would request a token first from the Token_Server,
            and then request a job from crawler_master server, at last crawler can use
            that token to start his job...excellent
        """
        if not self.token:
            print "will now get a new access_token from token Server"
#            self.token = self.get_token()
            #===================================================================
            # # token of stevecreateswarm@gmail.com
            # #self.token = "2.00nE3C_Dm8KADDfe233f8d32DOf4ZC"
            # # token of swarmben@126.com
            # #self.token = "2.00x4rH4Dm8KADD4199f9bf9fja8BKD"
            # # token of cnjswangheng66@yahoo.com.cn
            # #self.token = "2.00Ud5ucCm8KADDf043a319d4ZCvOmD"
            # # token of swarmbenben@126.com
            # #self.token = "2.00nZsH4Dm8KADDa420d7c617AWVJ2B"
            # # token of swarmweibo@126.com
            # #self.token = "2.00p75p3Dm8KADDefcd1c02d4hpxKiD"
            # # token of swarmheng@126.com
            # #self.token = "2.00BbvH4Dm8KADD753a244427EnkIjD"
            # 
            #===================================================================
            # # token of weiboreach cnjswangheng66@gmail.com: 
            self.token = "2.00iquaqBRbe3eC17ac477f57PKnbgB"
            #===================================================================
            # # # token of weiboreach cnjswangheng66@yahoo.com.cn: 
            #self.token = "2.00Ud5ucCRbe3eCa9dbefa08cGSV7SC"          
            #===================================================================
            #===================================================================
            # # # token of weiboreach swarmben@126.com: 
            #self.token = "2.00x4rH4DRbe3eC65a344abbb0ZtvIi"
            #===================================================================
            #===================================================================
            # # # token of weiboreach swarmbenben@126.com: 
            #self.token = "2.00nZsH4DRbe3eCafeb5c0e6eDdZU9E"          
            #===================================================================
        access_token_str = "access_token is: %s" % self.token
        print access_token_str
        self.logger.debug(access_token_str)
        if self.token:
            self.client = weibo.APIClient(self.token)
            while 1:
                res_json = self.request_job()
                print res_json['crawler_name']
                if res_json.has_key('job_json'): 
                    if res_json['job_json'] == None:
                        print "no job right now..."
                        no_job_sleep_seconds = 10
                        print "take a rest for %s seconds" % no_job_sleep_seconds
                        time.sleep(no_job_sleep_seconds)
                    else:
                        to_deliver = self.crawl_by_weibo_api(res_json['job_json'])
                        self.deliver_job(to_deliver)
                        self.send_weibo_log(res_json['job_json'])
#                    sleep_seconds = float(self.sleep_min) * 60
#                    time.sleep(sleep_seconds)
                else:
                    no_job_json_error = ('Has no such job_json ...')
                    raise self.error_handler.JobError(no_job_json_error)
        else:
            no_token_error_str = 'Has no token, get token error maybe...'
            raise self.error_handler.TokenError(no_token_error_str)
        

class ErrorHandler():
    """
    handle all kinds of errors..
    """
    def __init__(self, logger):
        self.logger = logger
        pass
    
    def print_logger_error(self, error_str):
        self.logger.error(error_str)
        print error_str
        print "now sleeping for 120 seconds"
        time.sleep(120)
        

    class ServerClosed(Exception):
        def __init__(self, current_status):
            self.current_status = current_status
            
        def __str__(self):
            return repr(self.current_status)
        
        def handle_server_closed(self):
            server_closed_str = " \
            Oops, the server is not running right now....\n \
            will rest for 120 seconds and then restart \n \
            Please contact swarm:  iswangheng@gmail.com" 
            print server_closed_str
            self.crawler.logger.error(server_closed_str)
            time.sleep(120)
        

    class JobError(Exception):
        def __init__(self, error_str):
            self.error_str = error_str
            
        def __str__(self):
            return repr(self.error_str)
        
        def handle_job_error(self):
            job_error_str = ('%s, will rest for 10 seconds and then restart crawling' % self.error_str)
            print job_error_str
            self.crawler.logger.error(job_error_str)
            time.sleep(10)
        

    class TokenError(Exception):
        def __init__(self, error_str):
            self.error_str = error_str
            
        def __str__(self):
            return repr(self.error_str)
        
        def handle_token_error(self):
            token_error_str = ('%s, will rest for 120 seconds and then restart crawling' % self.error_str)
            print token_error_str
            self.crawler.logger.error(token_error_str)
            time.sleep(120)
        
        
    class WeiboAPIError(Exception):
        def __init__(self, api_error_code, api_error_str, api_error_request, crawler):
            self.api_error_code = api_error_code
            self.api_error_str = api_error_str
            self.api_error_request = api_error_request
            self.weibo_api_error_str = "ErrorCode: %s | ErrorStr: %s | ErrorRequest: %s" \
                                  % (self.api_error_code, self.api_error_str, \
                                     self.api_error_request)
            self.crawler = crawler
            
        def __str__(self):
            return repr(self.weibo_api_error_str)
            
        def handle_api_error(self):
            try:
                self.crawler.token = None
                api_error_str = ('Expired token! will rest for 2 seconds and then restart crawling')
                if (self.api_error_code in (10022, 10023, 10024)):
                    #===============================================================
                    # error_code: 10022   ----->  IP address request out of limit
                    # error_code: 10023   ----->  User request out of limit
                    # error_code: 10024   ----->  User request %s interface out of limit
                    # need to tell the token_SERVER that this token is out of limit:
                    #   eg. http://csz908.cse.ust.hk/auth/token/limit?access_token=2.00nE3C_Dm8KADDb7ac378a1a0GJE6q
                    #===============================================================
                    api_error_str = ('error_code:%s Out of limit, will rest for 2 seconds and then restart crawling') \
                                    % (self.api_error_code)
                    self.crawler.limit_expire_token(limit_or_expire="limit", access_token=self.crawler.token)
                elif (self.api_error_code in (21325, 21327, 21501)):
                    #===============================================================================
                    # #21325    --->    the given Access Grant is invalid, expired or unauthorized 
                    # #21327    --->    token expired
                    # #21501    --->    access_token is invalid
                    # need to tell the token_SERVER that this token is expired:
                    #   eg. http://csz908.cse.ust.hk/auth/token/expire?access_token=2.00nE3C_Dm8KADDb7ac378a1a0GJE6q
                    #===============================================================
                    api_error_str = ('error_code:%s Expired token! will rest for 2 seconds and then restart crawling') \
                                    % (self.api_error_code)
                    self.crawler.limit_expire_token(limit_or_expire="expire", access_token=self.crawler.token)
                else:
                    # default error handler for the API error 
                    api_error_str = ('Weibo API error_code:%s will rest for 2 seconds and then restart crawling') \
                                    % (self.api_error_code)
                    self.crawler.limit_expire_token(limit_or_expire="expire", access_token=self.crawler.token)
            except:
                error_str = "when trying to tell the token_server: limit or expire token error"
                print error_str
                self.crawler.logger.error(error_str)
            finally:
                print api_error_str
                self.crawler.logger.error(api_error_str)
                time.sleep(2)
    
    
def crawler_start(crawler):
    try:
        crawler.start()
    except urllib2.HTTPError, e:
        http_error = ("HTTPError..error_code: %s" % (e.code))
        print (http_error)
        crawler.logger.error(http_error)
    except urllib2.URLError, e:
        if hasattr(e.reason, "errno"):
            crawler.logger.error("URLError %s " % (e.reason) )
            try:
                if e.reason.errno == 111:
                    raise crawler.error_handler.ServerClosed("")
            except crawler.error_handler.ServerClosed, e:
                e.handle_server_closed()
    except crawler.error_handler.TokenError, e:
        e.handle_token_error()
    except crawler.error_handler.JobError, e:
        e.handle_job_error()
    except crawler.error_handler.WeiboAPIError, e:
        crawler.logger.error(e.weibo_api_error_str)
        e.handle_api_error()
    except:
        error_str = ('%s' % (sys.exc_info()[0]) )
        crawler.logger.error(error_str)
        print error_str
    finally:
        crawler_start(crawler)

def main():
    crawler = Crawler()
    if crawler.check_version():
        print 'Check_Version: Okay..The crawler is an updated one.'
        crawler_start(crawler)
    else:
        version_old_str = " \
        Version Too Old, Please Update Your Crawler.\n \
        You can download here http://csz908.cse.ust.hk/crawler_master/download/  \n \
        Or just contact swarm:  iswangheng@gmail.com"
        crawler.error_handler.print_logger_error(version_old_str)
        
def test():
    crawler = Crawler()
    if not crawler.token:
        print "will now get a new access_token from token Server"
        crawler.token = crawler.get_token()
    if crawler.token:
        crawler.client = weibo.APIClient(crawler.token)
        while 1:
            follow_response = crawler.crawl_follow('1335280')
    else:
        no_token_error_str = 'Has no token, get token error maybe...'
        print no_token_error_str

if __name__ == "__main__":
    main()
#    test()
    