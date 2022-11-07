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
            g_clones += [zef.pyzef.internals.create_partial_graph(g.graph_data, g.graph_data.write_head)]
            [ET.Machine]*10 | g | run

        # Add in a couple of manual edge cases
        g_clones += [zef.pyzef.internals.create_partial_graph(g.graph_data, g.graph_data.write_head)]
        z = ET.Machine | g | run
        (z, RT.Something, 1) | g | run
        g_clones += [zef.pyzef.internals.create_partial_graph(g.graph_data, g.graph_data.write_head)]
        [(z, RT.Something, z), (z, RT.Something2, z)] | g | run
        g_clones += [zef.pyzef.internals.create_partial_graph(g.graph_data, g.graph_data.write_head)]
        z | terminate | g | run
        g_clones += [g]

        for i,tx in enumerate(g | all[TX] | skip[1] | collect):
            g_clone_before_tx = g_clones[i]

            before_heads = zef.internals.create_update_heads(g_clone_before_tx.graph_data)
            before_payload = zef.internals.create_update_payload(g_clone_before_tx.graph_data, before_heads, "")
            for j in range(i+1,len(g_clones)):
                g_partial = zef.pyzef.internals.create_partial_graph(g_clones[j].graph_data, g_clone_before_tx.graph_data.write_head)
                self.assertEqual(g_clone_before_tx.graph_data.hash(), g_partial.graph_data.hash())

                after_payload = zef.internals.create_update_payload(g_partial.graph_data, before_heads, "")
                self.assertEqual(before_payload, after_payload)

    def test_partial_during_transaction(self):
        g = Graph()

        a,b,c = (ET.Machine, RT.Something, 5) | g | run
        hash_before = g.hash()
        head_before = g.graph_data.read_head
        with Transaction(g):
            a | terminate | g | run
            [ET.Machine]*10 | g | run
            a,b,c = (ET.Machine, RT.Something, 5) | g | run
            g_clone_in_tx = zef.pyzef.internals.create_partial_graph(g.graph_data, head_before)
            hash_in_tx = zef.pyzef.internals.partial_hash(g, head_before)
        g_clone_after = zef.pyzef.internals.create_partial_graph(g.graph_data, head_before)
        hash_after = zef.pyzef.internals.partial_hash(g, head_before)
            
        self.assertEqual(g_clone_after.graph_data.hash(), hash_before)
        self.assertEqual(g_clone_in_tx.graph_data.hash(), hash_before)
        self.assertEqual(hash_after, hash_before)
        self.assertEqual(hash_in_tx, hash_before)

    def test_partial_from_merges(self):
        g = Graph()
        g2 = Graph()

        a,b,c = (ET.Machine, RT.Something, 5) | g | run
        a2,b2,c2 = (ET.Machine, RT.Something, 5) | g2 | run
        
        hash_before = g.hash()
        head_before = g.graph_data.read_head
        with Transaction(g):
            a | terminate | g | run
            [ET.Machine]*10 | g | run
            a3,b3,c3 = [a2,b2,c2] | g | run
        with Transaction(g):
            terminate(a3) | g | run
        g_clone_after = zef.pyzef.internals.create_partial_graph(g.graph_data, head_before)
        hash_after = zef.pyzef.internals.partial_hash(g, head_before)
            
        self.assertEqual(g_clone_after.graph_data.hash(), hash_before)
        self.assertEqual(hash_after, hash_before)

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
