# -*- coding: utf-8 -*-

# Copyright 2012 Renzo S.
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

"""Test the collection module.
Based on pymongo driver's test_collection.py
"""
from asyncio_mongo._pymongo import errors
from asyncio_mongo.collection import Collection
from asyncio_mongo._bson.son import SON
from asyncio_mongo import filter

from tests.base import MongoTest, async


class TestCollection(MongoTest):
    
    @async
    def test_collection(self):
        self.assertRaises(TypeError, Collection, self.db, 5)

        def make_col(base, name):
            return base[name]

        self.assertRaises(errors.InvalidName, make_col, self.db, "")
        self.assertRaises(errors.InvalidName, make_col, self.db, "te$t")
        self.assertRaises(errors.InvalidName, make_col, self.db, ".test")
        self.assertRaises(errors.InvalidName, make_col, self.db, "test.")
        self.assertRaises(errors.InvalidName, make_col, self.db, "tes..t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "te$t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, ".test")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "test.")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "tes..t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "tes\x00t")

        self.assert_(isinstance(self.db.test, Collection))
        self.assertEqual(self.db.test, Collection(self.db, "test"))
        self.assertEqual(self.db.test.mike, self.db["test.mike"])
        self.assertEqual(self.db.test["mike"], self.db["test.mike"])

        yield from self.db.drop_collection('test')
        collection_names = yield from self.db.collection_names()
        self.assertFalse('test' in collection_names)


    @async
    def test_create_index(self):
        db = self.db
        coll = self.coll

        self.assertRaises(TypeError, coll.create_index, 5)
        self.assertRaises(TypeError, coll.create_index, {"hello": 1})

        yield from coll.drop_indexes()
        count = yield from db.system.indexes.count({"ns": u"mydb.mycol"})
        self.assertEqual(count, 1)

        result1 = yield from coll.create_index(filter.sort(filter.ASCENDING("hello")))
        result2 = yield from coll.create_index(filter.sort(filter.ASCENDING("hello") + \
                                          filter.DESCENDING("world")))

        count = yield from db.system.indexes.count({"ns": u"mydb.mycol"})
        self.assertEqual(count, 3)

        yield from coll.drop_indexes()
        ix = yield from coll.create_index(filter.sort(filter.ASCENDING("hello") + \
                                   filter.DESCENDING("world")), name="hello_world")
        self.assertEquals(ix, "hello_world")

        yield from coll.drop_indexes()
        count = yield from db.system.indexes.count({"ns": u"mydb.mycol"})
        self.assertEqual(count, 1)
        
        yield from coll.create_index(filter.sort(filter.ASCENDING("hello")))
        indices = yield from db.system.indexes.find({"ns": u"mydb.mycol"})
        self.assert_(u"hello_1" in [a["name"] for a in indices])

        yield from coll.drop_indexes()
        count = yield from db.system.indexes.count({"ns": u"mydb.mycol"})
        self.assertEqual(count, 1)

        ix = yield from coll.create_index(filter.sort(filter.ASCENDING("hello") + \
                                   filter.DESCENDING("world")))
        self.assertEquals(ix, "hello_1_world_-1")

    @async
    def test_create_index_nodup(self):
        coll = self.coll

        yield from coll.drop()
        yield from coll.insert({'b': 1})
        yield from coll.insert({'b': 1})

        self.assertRaises(errors.DuplicateKeyError, coll.create_index, filter.sort(filter.ASCENDING("b")), unique=True)


    @async
    def test_ensure_index(self):
        db = self.db
        coll = self.coll
        
        yield from coll.ensure_index(filter.sort(filter.ASCENDING("hello")))
        indices = yield from db.system.indexes.find({"ns": u"mydb.mycol"})
        self.assert_(u"hello_1" in [a["name"] for a in indices])

        yield from coll.drop_indexes()

    @async
    def test_index_info(self):
        db = self.db

        yield from db.test.drop_indexes()
        yield from db.test.remove({})

        yield from db.test.save({})  # create collection
        ix_info = yield from db.test.index_information()
        self.assertEqual(len(ix_info), 1)

        self.assert_("_id_" in ix_info)

        yield from db.test.create_index(filter.sort(filter.ASCENDING("hello")))
        ix_info = yield from db.test.index_information()
        self.assertEqual(len(ix_info), 2)
        
        self.assertEqual(ix_info["hello_1"], [("hello", 1)])

        yield from db.test.create_index(filter.sort(filter.DESCENDING("hello") + filter.ASCENDING("world")), unique=True)
        ix_info = yield from db.test.index_information()

        self.assertEqual(ix_info["hello_1"], [("hello", 1)])
        self.assertEqual(len(ix_info), 3)
        self.assertEqual([("world", 1), ("hello", -1)], ix_info["hello_-1_world_1"])
        # Unique key will not show until index_information is updated with changes introduced in version 1.7
        #self.assertEqual(True, ix_info["hello_-1_world_1"]["unique"])

        yield from db.test.drop_indexes()
        yield from db.test.remove({})
        

    @async
    def test_index_geo2d(self):
        db = self.db
        coll = self.coll 
        yield from coll.drop_indexes()
        geo_ix = yield from coll.create_index(filter.sort(filter.GEO2D("loc")))

        self.assertEqual('loc_2d', geo_ix)

        index_info = yield from coll.index_information()
        self.assertEqual([('loc', '2d')], index_info['loc_2d'])

    @async
    def test_index_haystack(self):
        db = self.db
        coll = self.coll
        yield from coll.drop_indexes()

        _id = yield from coll.insert({
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        })
        yield from coll.insert({
            "pos": {"long": 34.2, "lat": 37.3}, "type": "restaurant"
        })
        yield from coll.insert({
            "pos": {"long": 59.1, "lat": 87.2}, "type": "office"
        })

        yield from coll.create_index(filter.sort(filter.GEOHAYSTACK("pos") + filter.ASCENDING("type")), **{'bucket_size': 1})

        # TODO: A db.command method has not been implemented yet.
        # Sending command directly
        command = SON([
            ("geoSearch", "mycol"),
            ("near", [33, 33]),
            ("maxDistance", 6),
            ("search", {"type": "restaurant"}),
            ("limit", 30),
        ])
           
        results = yield from db["$cmd"].find_one(command)
        self.assertEqual(2, len(results['results']))
        self.assertEqual({
            "_id": _id,
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        }, results["results"][0])

