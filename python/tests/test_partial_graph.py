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
import zef

class MyTestCase(unittest.TestCase):
    def test_partial_graphs_same(self):
        g = Graph()

        g_clones = []
        for i in range(10):
            g_clones += [zef.pyzef.internals.create_partial_graph(g, g.graph_data.write_head)]
            [ET.Machine]*10 | g | run

        # Add in a couple of manual edge cases
        g_clones += [zef.pyzef.internals.create_partial_graph(g, g.graph_data.write_head)]
        z = ET.Machine | g | run
        g_clones += [zef.pyzef.internals.create_partial_graph(g, g.graph_data.write_head)]
        [(z, RT.Something, z), (z, RT.Something2, z)] | g | run
        g_clones += [zef.pyzef.internals.create_partial_graph(g, g.graph_data.write_head)]
        z | terminate | g | run
        g_clones += [g]

        for i,tx in enumerate(g | all[TX] | skip[1] | collect):
            g_clone_before_tx = g_clones[i]

            for j in range(i+1,len(g_clones)):
                g_partial = zef.pyzef.internals.create_partial_graph(g_clones[j], g_clone_before_tx.graph_data.write_head)
                self.assertEqual(g_clone_before_tx.hash(), g_partial.hash())

    def test_abort_transaction(self):
        g = Graph()

        z = ET.Machine | g | run

        before_hash = g.hash()

        with Transaction(g):
            z2 = ET.Machine | g | run
            zef.pyzef.internals.AbortTransaction(g)
        self.assertEqual(before_hash, g.hash())

        with Transaction(g):
            (z, RT.Something, 5) | g | run
            zef.pyzef.internals.AbortTransaction(g)
        self.assertEqual(before_hash, g.hash())

        with Transaction(g):
            z | terminate | g | run
            zef.pyzef.internals.AbortTransaction(g)
        self.assertEqual(before_hash, g.hash())

        dz = delegate_of(z)
        (dz, RT.Meta, 1) | g | run
        before_hash = g.hash()

        with Transaction(g):
            (dz, RT.Something, 5) | g | run
            zef.pyzef.internals.AbortTransaction(g)
        self.assertEqual(before_hash, g.hash())

        z,_,ae = (z, RT.Something, 1) | g | run
        before_hash = g.hash()

        with Transaction(g):
            ae | assign[2] | g | run
            zef.pyzef.internals.AbortTransaction(g)
        self.assertEqual(before_hash, g.hash())

    def test_empty_transaction(self):
        g = Graph()

        num_txs = len(g | all[TX] | collect)
        before_read_head = g.graph_data.read_head

        with Transaction(g, True, True):
            pass

        self.assertEqual(num_txs, len(g | all[TX] | collect))
        self.assertEqual(before_read_head, g.graph_data.read_head)

        with Transaction(g, True, False):
            pass

        self.assertEqual(True, num_txs < len(g | all[TX] | collect))
        self.assertEqual(True, before_read_head < g.graph_data.read_head)

        

if __name__ == '__main__':
    unittest.main()
