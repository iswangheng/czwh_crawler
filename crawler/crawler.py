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
        self.crawler_master_url = self.config.get('crawler', 'master_url')
        self.token_server_url = self.config.get('crawler', 'token_server_url')
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
        except urllib2.URLError, e:
            self.logger.error("URLError")
            self.logger.error(e.reason.errno)
            if e.reason.errno == 111:
                raise ServerClosed("check_version")
        except urllib2.HTTPError, e:
            self.logger.error("HTTPError")
            self.logger.error(e.code)
        except:
            self.logger.error('unexpected error of check_version')
            self.logger.error("%s: %s" % (sys.exc_info()[0]), sys.exc_info()[1])
        else:
            version_res_json = simplejson.load(r)
            version_accept = version_res_json['version_accept']
        return version_accept
        pass
    
    def get_token(self):
        """
        will ask the token_server to get one token to start crawling
        """
        access_token = None
        token_url = self.token_server_url + 'get'
        try:
            req = urllib2.Request(url=token_url)
            r = urllib2.urlopen(req)
        except urllib2.URLError, e:
            self.logger.error("URLError")
            self.logger.error(e.reason.errno)
            if e.reason.errno == 111:
                raise ServerClosed("get_token()")
        except urllib2.HTTPError, e:
            self.logger.error(e)
        except:
            self.logger.error('unexpected error of get_token()')
            self.logger.error("%s: %s" % (sys.exc_info()[0]), sys.exc_info()[1])
        else:
            res_json = simplejson.load(r)
            access_token = res_json['access_token']
        return access_token

    def get_job(self):
        """
        will ask the crawler_master to get one job to start crawling
        """
        pass
    
    def start(self):
        """
        the crawler would start  
        """
        self.token = self.get_token()
        if self.token:
            pass
        else:
            self.logger.error('Have no token, get token error maybe...')
            pass
        pass
    

class ServerClosed(Exception):
    def __init__(self, current_status):
        self.current_status = current_status
        
    def __str__(self):
        return repr(self.current_status)
    
    
def main():
    crawler = Crawler()
    try:
        crawler.start()
        if crawler.check_version():
            crawler.start()
            pass
        else:
            version_old_str = " \
            Version Too Old, Please Update Your Crawler.\n \
            You can download here http://csz908.cse.ust.hk/crawler_master/download/  \n \
            Or just contact swarm:  iswangheng@gmail.com"
            crawler.logger.error(version_old_str)
            print version_old_str
            pass
    except ServerClosed, e:
        server_closed_str = " \
        Something bad just happens when crawler is %s  \n \
        Oops, the server is not running right now....\n \
        Please contact swarm:  iswangheng@gmail.com" % (e.current_status)
        crawler.logger.error(server_closed_str)
        print server_closed_str
        pass
    except:
        crawler.logger.error("Unexpected Error when running crawler")
        pass

if __name__ == "__main__":
    main()
    