import redis
import os

class RedisConnection():
    def __init__(self):
        self.redis_host = os.environ.get('REDIS_HOST')
        self.redis_port = int(os.environ.get('REDIS_PORT'))
        self.redis_password = os.environ.get('REDIS_PASSWORD')
        self.redis_db = os.environ.get('REDIS_DB')
    def connection(self):
        return redis.Redis(host=self.redis_host, port=self.redis_port, password=self.redis_password, db=self.redis_db)