#!/usr/bin/env python
# coding: utf-8
import asyncio

import asyncio_mongo
from asyncio_mongo._bson import DBRef


@asyncio.coroutine
def example():
    mongo = yield from asyncio_mongo.Connection.create('localhost', 27017)

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    doc_a = {"username":"foo", "password":"bar"}
    result = yield from test.insert(doc_a, safe=True)

    doc_b = {"settings":"foobar", "owner":DBRef("test", result)}
    yield from test.insert(doc_b, safe=True)

    doc = yield from test.find_one({"settings":"foobar"})
    print("doc is:", doc)

    if isinstance(doc["owner"], DBRef):
        ref = doc["owner"]
        owner = yield from foo[ref.collection].find_one(ref.id)
        print("owner:", owner)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(example())
