"""wrappers for Redis insert commands"""
import sys
from csv import reader
from itertools import groupby

import redis


class RedisImport(object):

    def __init__(self, **kwargs):
    	self.redis = redis.Redis()
    	self.pipeline = self.redis.pipeline()
    	self.batch_size = kwargs.get('batch_size', 1000)

    def load_set(self, key, IN, **kwargs):
        """
        """
        seen = set([None])
        reader_files = reader(IN, delimiter='\t')

        #TODO support multiple member add for Redis 2.4+
        for i, (member, _) in enumerate(groupby(reader_files,
        lambda x: x[0] if len(x) else None)):
            if member not in seen:
                self.pipeline.sadd(key, member.rstrip())
                seen.add(member)
                if not i % self.batch_size:
                    self.pipeline.execute()
        #send the last batch
        self.pipeline.execute()


    def load_list(self, key, IN, **kwargs):
        """
        """
        count = 0

        for line in IN:
            self.pipeline.rpush(key, line.rstrip())
            count += 1
            if not count % self.batch_size:
                self.pipeline.execute()
        #send the last batch
        self.pipeline.execute()


    def load_hash_list(IN, **kwargs):
        """
        """
        count = 0

        for key, mapping in IN:
            self.pipeline.hmset(key, mapping)
            count += 1
            if not count % self.batch_size:
                self.pipeline.execute()
        #send the last batch
        pipeline.execute()
