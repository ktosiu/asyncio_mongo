# coding: utf-8
# Copyright 2010 Mark L.
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
from tests.base import async, MongoTest


class TestMongoQueries(MongoTest):

    @async
    def test_SingleCursorIteration(self):
        yield from self.coll.insert([{'v':i} for i in range(10)], safe=True)
        res = yield from self.coll.find()
        self.assertEqual(len(res), 10)

    @async
    def test_MultipleCursorIterations(self):
        yield from self.coll.insert([{'v':i} for i in range(450)], safe=True)
        res = yield from self.coll.find()
        self.assertEqual(len(res), 450)

    @async
    def test_LargeData(self):
        yield from self.coll.insert([{'v':' '*(2**19)} for i in range(4)], safe=True)
        res = yield from self.coll.find()
        self.assertEqual(len(res), 4)


class TestMongoQueriesEdgeCases(MongoTest):

    @async
    def test_BelowBatchThreshold(self):
        yield from self.coll.insert([{'v':i} for i in range(100)], safe=True)
        res = yield from self.coll.find()
        self.assertEqual(len(res), 100)

    @async
    def test_EqualToBatchThreshold(self):
        yield from self.coll.insert([{'v':i} for i in range(101)], safe=True)
        res = yield from self.coll.find()
        self.assertEqual(len(res), 101)

    @async
    def test_AboveBatchThreshold(self):
        yield from self.coll.insert([{'v':i} for i in range(102)], safe=True)
        res = yield from self.coll.find()
        self.assertEqual(len(res), 102)


class TestLimit(MongoTest):

    @async
    def test_LimitBelowBatchThreshold(self):
        yield from self.coll.insert([{'v':i} for i in range(50)], safe=True)
        res = yield from self.coll.find(limit=20)
        self.assertEqual(len(res), 20)

    @async
    def test_LimitAboveBatchThreshold(self):
        yield from self.coll.insert([{'v':i} for i in range(200)], safe=True)
        res = yield from self.coll.find(limit=150)
        self.assertEqual(len(res), 150)

    @async
    def test_LimitAtBatchThresholdEdge(self):
        yield from self.coll.insert([{'v':i} for i in range(200)], safe=True)
        res = yield from self.coll.find(limit=100)
        self.assertEqual(len(res), 100)

        yield from self.coll.drop(safe=True)

        yield from self.coll.insert([{'v':i} for i in range(200)], safe=True)
        res = yield from self.coll.find(limit=101)
        self.assertEqual(len(res), 101)

        yield from self.coll.drop(safe=True)

        yield from self.coll.insert([{'v':i} for i in range(200)], safe=True)
        res = yield from self.coll.find(limit=102)
        self.assertEqual(len(res), 102)

    @async
    def test_LimitAboveMessageSizeThreshold(self):
        yield from self.coll.insert([{'v':' '*(2**20)} for i in range(8)], safe=True)
        res = yield from self.coll.find(limit=5)
        self.assertEqual(len(res), 5)

    @async
    def test_HardLimit(self):
        yield from self.coll.insert([{'v':i} for i in range(200)], safe=True)
        res = yield from self.coll.find(limit=-150)
        self.assertEqual(len(res), 150)

    @async
    def test_HardLimitAboveMessageSizeThreshold(self):
        yield from self.coll.insert([{'v':' '*(2**20)} for i in range(8)], safe=True)
        res = yield from self.coll.find(limit=-6)
        self.assertEqual(len(res), 4)


class TestSkip(MongoTest):

    @async
    def test_Skip(self):
        yield from self.coll.insert([{'v':i} for i in range(5)], safe=True)
        res = yield from self.coll.find(skip=3)
        self.assertEqual(len(res), 2)

        yield from self.coll.drop(safe=True)

        yield from self.coll.insert([{'v':i} for i in range(5)], safe=True)
        res = yield from self.coll.find(skip=5)
        self.assertEqual(len(res), 0)

        yield from self.coll.drop(safe=True)

        yield from self.coll.insert([{'v':i} for i in range(5)], safe=True)
        res = yield from self.coll.find(skip=6)
        self.assertEqual(len(res), 0)

    @async
    def test_SkipWithLimit(self):
        yield from self.coll.insert([{'v':i} for i in range(5)], safe=True)
        res = yield from self.coll.find(skip=3, limit=1)
        self.assertEqual(len(res), 1)

        yield from self.coll.drop(safe=True)

        yield from self.coll.insert([{'v':i} for i in range(5)], safe=True)
        res = yield from self.coll.find(skip=4, limit=2)
        self.assertEqual(len(res), 1)

        yield from self.coll.drop(safe=True)

        yield from self.coll.insert([{'v':i} for i in range(5)], safe=True)
        res = yield from self.coll.find(skip=4, limit=1)
        self.assertEqual(len(res), 1)

        yield from self.coll.drop(safe=True)

        yield from self.coll.insert([{'v':i} for i in range(5)], safe=True)
        res = yield from self.coll.find(skip=5, limit=1)
        self.assertEqual(len(res), 0)