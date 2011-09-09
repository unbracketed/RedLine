import unittest

from redis import Redis

from redline.commands import RedisImport


class TestImportSet(unittest.TestCase):

    def setUp(self):
        self.redis_client = Redis()
        self.redis_import = RedisImport()
        self.key = 'test_set'
        #make sure key doesn't exist already
        self.redis_client.delete(self.key)

    def tearDown(self):
        self.redis_client.delete(self.key)

    def test_load_set(self):
        """
        Test basic set import
        """
        somestuff = ['1', '2', '3', 'a', 'b', 'c']
        self.redis_import.load_set(self.key, somestuff)

        self.assertEquals(len(somestuff),
                self.redis_client.scard(self.key))

        self.assertEquals(self.redis_client.smembers(self.key),
                set(somestuff))

    def test_set_elements_unique(self):
        """
        Test that duplicate elements are removed in sets
        """

        #verify that repeated elements are removed
        repeats = ['x'] * 5

        self.redis_import.load_set(self.key, repeats)
        self.assertEquals(1,
                self.redis_client.scard(self.key))

    def test_non_validated(self):
        """
        Test that input with non-string types fails when validation is off
        """
        non_strings = [1, 2, 3]
        self.assertRaises(
                TypeError,
                self.redis_import.load_set,
                self.key,
                non_strings)

    def test_validated(self):
        """
        Test that validation handles mixed types
        """
        non_strings = [1, 2, 3]
        self.redis_import.load_set(self.key, non_strings, validate_input=True)
        self.assertEquals(3,
                self.redis_client.scard(self.key))
        self.assertEquals(set([str(x) for x in non_strings]),
                self.redis_client.smembers(self.key))
