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

# This file contains tests for things that were bugs at one point. Does not need
# to be organised at all

import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
import os
from zef import *
from zef.ops import *
import zef


class MyTestCase(unittest.TestCase):

    def test_linking_tx(self):
        g = Graph()

        # Get the first tx on the graph
        tx_node = g | all[TX] | first | collect

        r = [
            (tx_node, RT.User, "someone"),
        ] | transact[g] | run

        self.assertEqual("someone", tx_node | now | Out[RT.User] | value | collect)

        r = [
            (tx_node, RT.Something, g | root | collect),
        ] | transact[g] | run

        # Note: the root has delegates coming off of it, so we need to filter by only the concrete relation.
        self.assertEqual(g | now | root | in_rels[RT.Something] | filter[Not[is_a[Delegate]]] | single | collect,
                         tx_node | now | out_rel[RT.Something] | collect)

    def test_linking_with_transaction(self):
        g = Graph()

        with Transaction(g) as ctx:
            (ctx, RT.User, "Someone") | g | run

        self.assertEqual(g | all[TX] | last | now | Out[RT.User] | value | collect,
                         "Someone")

    def test_attaching_to_delegates(self):
        g = Graph()

        with Transaction(g) as ctx:
            (ctx, RT.User, "Someone") | g | run

        (delegate_of(ctx), RT.Fixed, True) | g | run
        # Note: True so that we create the delegate
        (g | root | delegate_of[True] | collect, RT.Root, True) | g | run
        # Note: trying alternative with abstract delegate
        (g | root | to_delegate | delegate_of | collect, RT.Second, True) | g | run

    def test_failure_on_other_graph(self):
        g = Graph()
        g2 = Graph()

        tx_node = g | all[TX] | first | collect
        # with self.assertRaisesRegex(Exception, "Can only merge TXNode and Root onto the same graph"):
        with self.assertRaises(Exception):
            (tx_node, RT.User, "someone") | g2 | run

if __name__ == '__main__':
    unittest.main()
