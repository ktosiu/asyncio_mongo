#!/usr/bin/env python
# coding: utf-8
from asyncio import coroutine
import asyncio

import asyncio_mongo

@coroutine
def example():
    mongo = yield from asyncio_mongo.Connection.create('localhost', 27017)

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    yield from test.insert({"src":"Twitter", "content":"bla bla"}, safe=True)
    yield from test.insert({"src":"Twitter", "content":"more data"}, safe=True)
    yield from test.insert({"src":"Wordpress", "content":"blog article 1"}, safe=True)
    yield from test.insert({"src":"Wordpress", "content":"blog article 2"}, safe=True)
    yield from test.insert({"src":"Wordpress", "content":"some comments"}, safe=True)

    # Read more about the aggregation pipeline in MongoDB's docs
    pipeline = [
        {'$group': {'_id':'$src', 'content_list': {'$push': '$content'} } }
    ]
    result = yield from test.aggregate(pipeline)

    print("result:", result)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(example())