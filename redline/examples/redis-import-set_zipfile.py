# redis-import-set
import sys
from itertools import count, groupby
from zipfile import ZipFile

import redis


if __name__ == '__main__':
	
    r = redis.Redis()
    pipeline_redis = r.pipeline()
    count = 0
    try:
        keyname = sys.argv[1]
    except IndexError:
        raise Exception("You must specify the name for the Set")

    zf = ZipFile(sys.stdin)
    arch = zf.namelist()[0]

    for k, _ in groupby(zf.open(arch), 
            lambda x:x.split('\t')[0]):
        pipeline_redis.sadd(keyname, k)
        count += 1
        if not count % 10000:
        	pipeline_redis.execute()


