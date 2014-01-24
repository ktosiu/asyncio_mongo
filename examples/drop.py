#!/usr/bin/env python
# coding: utf-8
import asyncio

import asyncio_mongo

@asyncio.coroutine
def example():
    mongo = yield from asyncio_mongo.Connection.create('localhost', 27017)

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    result = yield from test.drop(safe=True)
    print(result)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(example())
