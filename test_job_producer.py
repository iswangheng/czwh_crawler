'''
Created on 7 Nov, 2012
this is the unit test

@author: swarm
'''
import unittest
import job_producer
import job_type


class Test(unittest.TestCase):
    def setUp(self):
        self.producer = job_producer.Producer() 
        pass

    def tearDown(self):
        pass

    def test_query_update_following(self):
        """
        this function is just to test 
            the query_update_following() function 
                in Producer Class in job_producer Module
        """
        limit_num = 100
        user_id_list = self.producer.query_update_following(limit_num)
        self.assertEqual(len(user_id_list), limit_num, 'len_of(user_id_list) != limit_num')
        for user_id in user_id_list:
            self.assertIsInstance(user_id, long, 'user_id in user_id_list is not int type!')
        pass
    
    def test_get_job_queue_length(self):
        """
        just to test 
            the get_job_queue_length() function 
                in Producer Class in job_producer Module
        """
        length = self.producer.get_job_queue_length()
        print length
        self.assertIsInstance(length, dict, 'job_queue_length is not dict type!')
        pass
    
    def test_send_job(self):
        """
        just to test 
            the send_job() function 
                in Producer Class in job_producer Module
        job_type_int = job_type.job_dict['bi_follow_id']
        self.producer.produce_job(job_type_int, 100)
        self.producer.send_job()
        #@todo: to be continued here..
        """
        pass

    def test_start(self):
        """
        just to test 
            the start() function 
                in Producer Class in job_producer Module
        """
        self.producer.start()
        pass

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()