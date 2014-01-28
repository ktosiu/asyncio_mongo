import inspect
import unittest
from asyncio import coroutine
import asyncio
import asyncio_mongo

mongo_host = "localhost"
mongo_port = 27017


def yields(value):
    return isinstance(value, asyncio.futures.Future) or inspect.isgenerator(value)


@coroutine
def call_maybe_yield(func, *args, **kwargs):
    rv = func(*args, **kwargs)
    if yields(rv):
        rv = yield from rv
    return rv


def run_now(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        asyncio.Task(call_maybe_yield(func, *args, **kwargs))
    )


def async(func):
    def inner(*args, **kwargs):
        run_now(func, *args, **kwargs)
    return inner


class MongoTest(unittest.TestCase):

    @async
    def setUp(self):
        self.conn = yield from asyncio_mongo.Connection.create(mongo_host, mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol
        yield from self.coll.drop()

    @async
    def tearDown(self):
        yield from self.coll.drop()
        self.conn.disconnect()