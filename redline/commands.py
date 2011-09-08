"""wrappers for Redis insert commands"""
import sys
from csv import reader
from itertools import groupby, imap

import redis


class RedisImport(object):
    """
    Provides methods for importing lists of data into Redis data structures

    Optional Arguments

    batch_size      Number of items to send per Redis operation
    validate_input  If enabled, each input item will be cast to string
    """

    def __init__(self, **kwargs):
        self.redis = redis.Redis()
        self.pipeline = self.redis.pipeline()
        self.batch_size = kwargs.get('batch_size', 1000)
        self.validate_input = kwargs.get('validate_input', False)

    def load_set(self, key, IN, **kwargs):
        """
        Load a list of input into a Redis set.
        """
        seen = set([])

        validate = kwargs.get('validate_input', self.validate_input)
#FIXME naive conversion
        if validate:
            IN = imap(str, IN)

        reader_files = reader(IN, delimiter='\t')
        reader_counter = enumerate(groupby(reader_files,
            lambda x: x[0] if len(x) else None))
        batch_size = self.batch_size

        #TODO support multiple member add for Redis 2.4+
        for i, (member, _) in reader_counter:
            if member not in seen:
                self.pipeline.sadd(key, member.rstrip())
                seen.add(member)
                if not i % batch_size:
                    self.pipeline.execute()
        #send the last batch
        self.pipeline.execute()

    def load_list(self, key, IN, **kwargs):
        """
        Load a list of input into a Redis list
        """
        batch_size = self.batch_size
        for i, line in enumerate(IN):
            self.pipeline.rpush(key, line.rstrip())
            if not i % batch_size:
                self.pipeline.execute()
        #send the last batch
        self.pipeline.execute()

    def load_hash_list(IN, **kwargs):
        """
        Load a list of dicts into Redis hashes, one dict per hash
        """
        count = 0

        for key, mapping in IN:
            self.pipeline.hmset(key, mapping)
            count += 1
            if not count % self.batch_size:
                self.pipeline.execute()
        #send the last batch
        self.pipeline.execute()
