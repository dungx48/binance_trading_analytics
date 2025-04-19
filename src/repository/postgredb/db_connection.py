import psycopg2
import os

from functools import cached_property

class DatabaseConnection:
    @cached_property
    def connection(self):
        return psycopg2.connect(
            dbname=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT")
        )