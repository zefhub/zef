# Copyright 2022 Synchronous Technologies Pte Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *

class MyTestCase(unittest.TestCase):
    def test_zefrefs_from_slices(self):

        g = Graph()
        z = AET.String | g | run
        (z <= "a") | g | run
        (z <= "b") | g | run
        z | terminate | g | run

        g2 = Graph()
        z2 = z | g2 | run

        ctx = z | instantiated | collect
        # Note: slice 1 is created on Graph(), so this is slice 2
        self.assertEqual(ctx | to_graph_slice | time_slice | collect, TimeSlice(2))

        self.assertEqual(ctx | instantiated | length | collect, 1)
        self.assertEqual(ctx | value_assigned | length | collect, 0)
        self.assertEqual(ctx | terminated | length | collect, 0)
        self.assertEqual(ctx | merged | length | collect, 0)

        txs = z | now[allow_tombstone] | value_assigned | collect
        self.assertEqual([ctx | to_graph_slice | time_slice | collect for ctx in txs], [TimeSlice(3),TimeSlice(4)])
        for ctx in txs:
            self.assertEqual(ctx | instantiated | length | collect, 0)
            self.assertEqual(ctx | value_assigned | length | collect, 1)
            self.assertEqual(ctx | terminated | length | collect, 0)
            self.assertEqual(ctx | merged | length | collect, 0)
            

        ctx = z | terminated | collect
        self.assertEqual(ctx | to_graph_slice | time_slice | collect, TimeSlice(5))

        self.assertEqual(ctx | instantiated | length | collect, 0)
        self.assertEqual(ctx | value_assigned | length | collect, 0)
        self.assertEqual(ctx | terminated | length | collect, 1)
        self.assertEqual(ctx | merged | length | collect, 0)

        ctx = z2 | terminated | collect
        self.assertEqual(ctx, None)

        ctx = z2 | merged | collect
        # Note: slice 1 is created on Graph(), so this is slice 2
        self.assertEqual(ctx | to_graph_slice | time_slice | collect, TimeSlice(2))

        self.assertEqual(ctx | instantiated | length | collect, 1)
        self.assertEqual(ctx | value_assigned | length | collect, 0)
        self.assertEqual(ctx | terminated | length | collect, 0)
        self.assertEqual(ctx | merged | length | collect, 1)

if __name__ == '__main__':
    unittest.main()
