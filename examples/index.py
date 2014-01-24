#!/usr/bin/env python
# coding: utf-8
import asyncio

import asyncio_mongo
from asyncio_mongo import filter

@asyncio.coroutine
def example():
    mongo = yield from asyncio_mongo.Connection.create('localhost', 27017)

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    idx = filter.sort(filter.ASCENDING("something") + filter.DESCENDING("else"))
    print("IDX:", idx)

    result = yield from test.create_index(idx)
    print("create_index:", result)

    result = yield from test.index_information()
    print("index_information:", result)

    result = yield from test.drop_index(idx)
    print("drop_index:", result)

    # Geohaystack example
    geoh_idx = filter.sort(filter.GEOHAYSTACK("loc") + filter.ASCENDING("type"))
    print("IDX:", geoh_idx)
    result = yield from test.create_index(geoh_idx, **{'bucketSize':1})
    print("index_information:", result)
    
    result = yield from test.drop_index(geoh_idx)
    print("drop_index:", result)

    # 2D geospatial index
    geo_idx = filter.sort(filter.GEO2D("pos"))
    print("IDX:", geo_idx)
    result = yield from test.create_index(geo_idx, **{ 'min':-100, 'max':100 })
    print("index_information:", result)

    result = yield from test.drop_index(geo_idx)
    print("drop_index:", result)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(example())
