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
import json
import simplejson


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
        """ will read producer_config.ini and then return the config for further use
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
        try:
            req = urllib2.Request(url=check_version_url, \
                                  data=version_json, \
                                  headers={'Content-Type': 'application/json'})
            r = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            http_error = ("check_version..HTTPError..error_code: %s" % (e.code))
            self.error_handler.print_logger_error(http_error)
        except urllib2.URLError, e:
            if hasattr(e.reason, "errno"):
                self.logger.error("URLError %s " % (e.reason) )
                if e.reason.errno == 111:
                    raise self.error_handler.ServerClosed("check_version()")
        except:
            unexpected_error = ('Unexpected error of check_version %s: ' % (sys.exc_info()[0]))
            self.error_handler.print_logger_error(unexpected_error)
        else:
            try:
                version_res_json = simplejson.load(r)
                version_accept = version_res_json['version_accept']
            except:
                self.logger.error("version_accept load json error..")
        return version_accept
    
    def get_token(self):
        """
        will ask the token_server to get one token to start crawling
        """
        access_token = None
        token_url = self.token_server_url + 'get'
        try:
            req = urllib2.Request(url=token_url)
            r = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            http_error = ("get_token..HTTPError..error_code: %s" % (e.code))
            self.error_handler.print_logger_error(http_error)
        except urllib2.URLError, e:
            if hasattr(e.reason, "errno"):
                self.logger.error("URLError %s " % (e.reason) )
                if e.reason.errno == 111:
                    raise self.error_handler.ServerClosed("get_token()")
        except:
            self.logger.error('unexpected error of get_token()')
            self.logger.error("%s" % (sys.exc_info()[0]))
        else:
            res_json = simplejson.load(r)
            access_token = res_json['access_token']
        return access_token

    def request_job(self):
        """
        will ask the crawler_master to get one job to start crawling
        """
        request_job_url = self.crawler_master_url + 'request_job/'
        request_dict = {'crawler_name': self.crawler_name}
        request_json= json.dumps(request_dict)
        res_json = None
        try:
            req = urllib2.Request(url=request_job_url, \
                                  data=request_json, \
                                  headers={'Content-Type': 'application/json'})
            r = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            http_error = ("request_job..HTTPError..error_code is: %s" % (e.code))
            self.error_handler.print_logger_error(http_error)
        except urllib2.URLError, e:
            if hasattr(e.reason, "errno"):
                self.logger.error("URLError %s " % (e.reason) )
                if e.reason.errno == 111:
                    raise self.error_handler.ServerClosed("request_job()")
        except:
            unexpected_error = ('Unexpected error of request_job %s' % (sys.exc_info()[0]))
            self.error_handler.print_logger_error(unexpected_error)
        else:
            try:
                res_json = simplejson.load(r)
            except:
                error_str = ('Error of simplejson.load %s' % (sys.exc_info()[0]))
                self.error_handler.print_logger_error(error_str)
        return res_json
    
    def deliver_job(self, to_deliver):
        """
        will deliver the completed job to crawler_master
        @param to_deliver: a dict object 
        """
        deliver_job_url = self.crawler_master_url + 'deliver_job/'
        to_deliver.update({'crawler_name': self.crawler_name})
        to_deliver_json = json.dumps(to_deliver)
        res_json = None
        try:
            req = urllib2.Request(url=deliver_job_url, \
                                  data=to_deliver_json, \
                                  headers={'Content-Type': 'application/json'})
            r = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            http_error = ("deliver_job..HTTPError..error_code is: %s" % (e.code))
            self.error_handler.print_logger_error(http_error)
        except urllib2.URLError, e:
            if hasattr(e.reason, "errno"):
                self.logger.error("URLError %s " % (e.reason) )
                if e.reason.errno == 111:
                    raise self.error_handler.ServerClosed("deliver_job()")
        except:
            unexpected_error = ('Unexpected error of deliver_job %s' % (sys.exc_info()[0]))
            self.error_handler.print_logger_error(unexpected_error)
        else:
            self.logger.info(r.read())
            print r.read()
        return res_json
    
    def crawl_follow(self, user_id):
        """
        would start crawling the user_id's follow relation through Sina Weibo API 
        @param user_id:  
        @return: the json returned by sina weibo
        """
        print "will crawl %s follow " % user_id
        next_cursor = 0
        while 1:
            try:
                follow_response = self.client.friendships__friends(uid=user_id, count=200, cursor=next_cursor)
                follow_json = json.loads(follow_response)
            except urllib2.HTTPError, e:
                http_error = ("HTTPError..crawl_follow error_code is: %s" % (e.code))
                self.error_handler.print_logger_error(http_error)
            except urllib2.URLError, e:
                if hasattr(e.reason, "errno"):
                    error_str = ('URLError crawl_follow %s' % (e.reason.errno) )
                    self.error_handler.print_logger_error(error_str)
            except weibo.APIError, api_error:
                #@todo
                # all kinds of errors: out_of_limit, token_expired, invalid_access_token
                error_str = ('crawl_follow API Error %s %s %s' % (api_error.error_code, 
                                                                  api_error.error,
                                                                  api_error.request) )
                self.error_handler.print_logger_error(error_str)
                pass
            except:
                error_str = ('crawl_follow %s %s' % (sys.exc_info()[0], sys.exc_info()[1]))
                self.error_handler.print_logger_error(error_str)
                pass
            else:
                try:
                    next_cursor = follow_json['next_cursor']
                except:
                    self.logger.error("%s has no next_cursor while crawling follow" % (user_id) )
                    break
                else:
                    if not next_cursor:
                        break
        return follow_json 
        
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
        except urllib2.HTTPError, e:
            http_error = ("HTTPError..crawl_bi_follow_id error_code is: %s" % (e.code))
            self.error_handler.print_logger_error(http_error)
        except urllib2.URLError, e:
            if hasattr(e.reason, "errno"):
                error_str = ('URLError crawl_bi_follow_id %s' % (e.reason.errno) )
                self.error_handler.print_logger_error(error_str)
        except weibo.APIError, api_error:
            #@todo
            # all kinds of errors: out_of_limit, token_expired, invalid_access_token
            error_str = ('crawl_follow API Error %s %s %s' % (api_error.error_code, api_error.error. api_error.request) )
            self.error_handler.print_logger_error(error_str)
            pass
        except:
            #@todo
            # all kinds of errors: out_of_limit, token_expired, invalid_access_token
            error_str = ('crawl_bi_follow_id %s %s' % (sys.exc_info()[0], sys.exc_info()[1]))
            self.error_handler.print_logger_error(error_str)
            pass
        return friends_ids_json 
        
            
    def crawl_by_weibo_api(self, job_json):
        """
        would start crawling through Sina Weibo API based on the info from job_json
        @param job_json:  = {"job_type": **, "user_id":**} 
        @return: the json returned by sina weibo
        """
        to_deliver = {'job_type': job_json['job_type']}
        if job_json['job_type'] == job_const.JOB_TYPE_BI_FOLLOW_ID:
            if job_json.has_key('user_id'):
                friends_ids_response = self.crawl_bi_follow_id(job_json['user_id'])
                to_deliver.update({'user_id': job_json['user_id']})
                to_deliver.update({'sina_weibo_json':friends_ids_response})
                pass
            else:
                #@todo
                pass
        elif job_json['job_type'] == job_const.JOB_TYPE_FOLLOW:
            if job_json.has_key('user_id'):
                #@todo
                follow_response = self.crawl_follow(job_json['user_id'])
                to_deliver.update({'user_id': job_json['user_id']})
                to_deliver.update({'sina_weibo_json': follow_response})
                pass
            else:
                #@todo
                pass
        elif job_json['job_type'] == job_const.JOB_TYPE_USER_WEIBO:
            pass
        return to_deliver
    
    def start(self):
        """
        the crawler would start AFTER the version has been checked  
            In this part: the crawler would request a token first from the Token_Server,
            and then request a job from crawler_master server, at last crawler can use
            that token to start his job...excellent
        """
#        self.token = self.get_token()
        #=======================================================================
        # out of limit: 2.00x4rH4Dm8KADD16f4445920QoilXC 
        #=======================================================================
        self.token = ('2.00x4rH4Dm8KADD16f4445920QoilXC')
        if self.token:
            self.client = weibo.APIClient(self.token)
            while 1:
                res_json = self.request_job()
                print res_json['crawler_name']
                if res_json.has_key('job_json'): 
                    to_deliver = self.crawl_by_weibo_api(res_json['job_json'])
    #                self.deliver_job(to_deliver)
                else:
                    no_job_json_error = ('Has no such job_json ...')
                    self.error_handler.print_logger_error(no_job_json_error)
                    # @todo
                pass
        else:
            no_token_error_str = ('Has no token, get token error maybe...')
            self.error_handler.print_logger_error(no_token_error_str)
            pass
        pass
    

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
        while 1:
            pass

    class ServerClosed(Exception):
        def __init__(self, current_status):
            self.current_status = current_status
            
        def __str__(self):
            return repr(self.current_status)

    class WeiboAPIError(Exception):
        def __init__(self, current_status):
            self.current_status = current_status
            
        def __str__(self):
            return repr(self.current_status)
    
    
def main():
    crawler = Crawler()
    try:
        if crawler.check_version():
            print 'Check_Version: Okay..The crawler is an updated one.'
            crawler.start()
        else:
            version_old_str = " \
            Version Too Old, Please Update Your Crawler.\n \
            You can download here http://csz908.cse.ust.hk/crawler_master/download/  \n \
            Or just contact swarm:  iswangheng@gmail.com"
            crawler.error_handler.print_logger_error(version_old_str)
    except crawler.error_handler.ServerClosed, e:
        server_closed_str = " \
        Something bad just happens when crawler is %s  \n \
        Oops, the server is not running right now....\n \
        Please contact swarm:  iswangheng@gmail.com" % (e.current_status)
        crawler.error_handler.print_logger_error(server_closed_str)
    except:
        error_str = ('%s' % (sys.exc_info()[0]) )
        crawler.error_handler.print_logger_error(error_str)
        pass

if __name__ == "__main__":
    main()
    