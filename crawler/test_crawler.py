'''
Created on 9 Nov, 2012

@author: swarm
'''
import unittest
import crawler


class Test(unittest.TestCase):

    def setUp(self):
        self.crawler = crawler.Crawler() 
        pass

    def tearDown(self):
        pass

    def test_check_version(self):
        print self.crawler.check_version()
""" 
    def test_get_token(self):
        print self.crawler.get_token()
        pass
""" 


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()