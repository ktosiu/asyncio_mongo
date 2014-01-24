#!/usr/bin/env python
# coding: utf-8
import asyncio

import asyncio_mongo
import asyncio_mongo.filter

@asyncio.coroutine
def example():
    mongo = yield from asyncio_mongo.Connection.create('localhost', 27017)

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    # create the filter
    f = asyncio_mongo.filter.sort(asyncio_mongo.filter.DESCENDING("something"))
    #f += asyncio_mongo.filter.hint(asyncio_mongo.filter.DESCENDING("myindex"))
    #f += asyncio_mongo.filter.explain()

    # fetch some documents
    docs = yield from test.find(limit=10, filter=f)
    for n, doc in enumerate(docs):
        print(n, doc)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(example())
