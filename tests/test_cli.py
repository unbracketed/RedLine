import os
import unittest

import redis


class TestCommandLineInterface(unittest.TestCase):
    """
    Tests for the command line interface to RedLine
    """

    def setUp(self):
        self.data_dir = os.path.join(os.path.dirname(__file__), 'data')
        self.key = 'test_set'
        self.redis_client = redis.Redis()
        self.redis_client.delete(self.key)

    def tearDown(self):
        self.redis_client.delete(self.key)

    def test_import_set(self):
        """
        Test importing a set from a text file
        """
        cmd = "redis-import set %s < %s"
        testfile = os.path.join(self.data_dir, "words_short.txt")
        os.system(cmd % (self.key, testfile))
#FIXME verify data
