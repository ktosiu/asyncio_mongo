#!/usr/bin/env python
# coding: utf-8
import asyncio

import asyncio_mongo

@asyncio.coroutine
def example():
    mongo = yield from asyncio_mongo.Connection.create('localhost', 27017)

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    # fetch some documents
    docs = yield from test.find(limit=10)
    for doc in docs:
        print(doc)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(example())