# coding: utf-8
# Copyright 2009 Alexandre Fiori
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import inspect

import asyncio_mongo
from tests.base import MongoTest, async


mongo_host = "localhost"
mongo_port = 27017


class TestMongoConnectionMethods(MongoTest):
    
    @async
    def test_connection(self):
        # MongoConnection returns deferred, which gets MongoAPI
        conn = asyncio_mongo.Connection.create(mongo_host, mongo_port)
        self.assertTrue(inspect.isgenerator(conn))
        rapi = yield from conn
        self.assertEqual(isinstance(rapi, asyncio_mongo.Connection), True)
        rapi.disconnect()

    @async
    def test_pool(self):
        # MongoConnectionPool returns deferred, which gets MongoAPI
        pool = asyncio_mongo.Pool.create(mongo_host, mongo_port, poolsize=2)
        self.assertTrue(inspect.isgenerator(pool))
        rapi = yield from pool
        self.assertEqual(isinstance(rapi, asyncio_mongo.Pool), True)
        rapi.close()

    @async
    def test_pool(self):
        # MongoConnectionPool returns deferred, which gets MongoAPI
        pool = asyncio_mongo.Pool.create(mongo_host, "%s,blah:333" % mongo_port, poolsize=2)
        self.assertTrue(inspect.isgenerator(pool))
        rapi = yield from pool
        self.assertEqual(isinstance(rapi, asyncio_mongo.Pool), True)
        rapi.close()