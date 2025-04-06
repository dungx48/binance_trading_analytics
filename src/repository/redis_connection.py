import redis
import os

class RedisConnection():
    def __init__(self):
        self.redis_host = os.getenv('REDIS_HOST')
        self.redis_port = int(os.getenv('REDIS_PORT'))
        self.redis_password = os.getenv('REDIS_PASSWORD')
    def connection(self):
        return redis.Redis(host=self.redis_host, port=self.redis_port, password=self.redis_password, db=0)