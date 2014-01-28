# coding: utf-8
# Copyright 2010 Tryggvi Bjorgvinsson
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
from tests.base import MongoTest, async


class TestAggregate(MongoTest):

    timeout = 5

    @async
    def test_aggregate(self):
        yield from self.coll.insert([{'oh':'hai', 'lulz':123},
                                {'oh':'kthxbye', 'lulz':456},
                                {'oh':'hai', 'lulz':789},], safe=True)

        res = yield from self.coll.aggregate([
                {'$project': {'oh':1, 'lolz':'$lulz'}}, 
                {'$group': {'_id':'$oh', 'many_lolz': {'$sum':'$lolz'}}},
                {'$sort': {'_id':1}}
                ])

        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['_id'], 'hai')
        self.assertEqual(res[0]['many_lolz'], 912)
        self.assertEqual(res[1]['_id'], 'kthxbye')
        self.assertEqual(res[1]['many_lolz'], 456)

        res = yield from self.coll.aggregate([
                {'$match': {'oh':'hai'}}
                ], full_response=True)

        self.assertIn('ok', res)
        self.assertIn('result', res)
        self.assertEqual(len(res['result']), 2)
