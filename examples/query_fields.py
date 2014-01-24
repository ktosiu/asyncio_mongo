#!/usr/bin/env python
# coding: utf-8
import asyncio

import asyncio_mongo
import asyncio_mongo.filter

@asyncio.coroutine
def example():
    mongo = yield from asyncio_mongo.Connection.create('localhost', 27017)

    foo = mongo.foo # `foo` database
    test = foo.test # `test` collection

    # specify the fields to be returned by the query
    # reference: http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields
    whitelist = {'_id': 1, 'name': 1}
    blacklist = {'_id': 0}
    quickwhite = ['_id', 'name']

    fields = blacklist

    # fetch some documents
    docs = yield from test.find(limit=10, fields=fields)
    for n, doc in enumerate(docs):
        print(n, doc)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(example())
