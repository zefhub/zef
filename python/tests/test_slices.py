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
        z | assign["a"] | g | run
        z | assign["b"] | g | run
        z | terminate | g | run

        g2 = Graph()
        z2 = z | g2 | run

        ctx = z | instantiation_tx | collect
        # Note: slice 1 is created on Graph(), so this is slice 2
        self.assertEqual(ctx | to_graph_slice | graph_slice_index | collect, 2)

        self.assertEqual(ctx | events[Instantiated] | length | collect, 2)
        self.assertEqual(ctx | events[Assigned] | length | collect, 0)
        self.assertEqual(ctx | events[Terminated] | length | collect, 0)

        txs = z | now[allow_tombstone] | preceding_events[Assigned] | map[absorbed | first | frame | to_tx] | collect
        self.assertEqual([ctx  | frame | graph_slice_index | collect for ctx in txs], [3, 4])
        for ctx in txs:
            self.assertEqual(ctx | events[Instantiated] | length | collect, 1)
            self.assertEqual(ctx | events[Assigned] | length | collect, 1)
            self.assertEqual(ctx | events[Terminated] | length | collect, 0)
            

        ctx = z | now[allow_tombstone] | termination_tx | collect
        self.assertEqual(ctx | to_graph_slice | graph_slice_index | collect, 5)

        self.assertEqual(ctx | events[Instantiated] | length | collect, 0)
        self.assertEqual(ctx | events[Assigned] | length | collect, 0)
        self.assertEqual(ctx | events[Terminated] | length | collect, 1)

        ctx = z2 | termination_tx | collect
        self.assertEqual(ctx, g2[42])


if __name__ == '__main__':
    unittest.main()
