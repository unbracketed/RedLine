import unittest

from redis import Redis

from redline.commands import RedisImport


class TestImport(unittest.TestCase):

    def setUp(self):
        self.redis_client = Redis()
        self.redis_import = RedisImport()

    def test_load_set(self):
        """
        Test basic set import
        """
        key = 'test_set'
        somestuff = ['1', '2', '3', 'a', 'b', 'c']
        self.redis_import.load_set(key, somestuff)

        self.assertEquals(len(somestuff),
                self.redis_client.scard(key))

        self.assertEquals(self.redis_client.smembers(key),
                set(somestuff))

    def test_set_elements_unique(self):
    	"""
    	Test that duplicate elements are removed in sets
    	"""

        #verify that repeated elements are removed
        repeats = ['x'] * 5
        key2 = 'test_set2'

        self.redis_import.load_set(key2, repeats)
        self.assertEquals(1,
                self.redis_client.scard(key2))

    def test_handle_non_string_input(self):
    	"""
    	Test that input with mixed types is handled properly
    	"""
    	pass
