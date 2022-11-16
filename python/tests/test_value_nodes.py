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
from zef.core.VT.rae_types import RAET_get_token

class MyTestCase(unittest.TestCase):

    def test_value_node_reuse(self):
        g = Graph()

        z = zef.pyzef.main.instantiate_value_node(5, g)
        z2 = zef.pyzef.main.instantiate_value_node(5, g)

        self.assertEqual(z, z2)

        from zef.pyzef.zefops import SerializedValue
        s = SerializedValue("plain string", "asdf")
        z = zef.pyzef.main.instantiate_value_node(s, g)
        z2 = zef.pyzef.main.instantiate_value_node(s, g)

        self.assertEqual(z, z2)

    def test_value_node_graph_delta(self):
        g = Graph()

        z = Val(5) | g | run
        z2 = Val(5) | g | run

        self.assertEqual(z, z2)
        self.assertEqual(value(z), 5)

        z = Val({'some_dict': 5}) | g | run
        z2 = Val({'some_dict': 5}) | g | run

        self.assertEqual(z, z2)
        self.assertEqual(value(z), {'some_dict': 5})

        a,b,c = (ET.Machine, RT.Kind, Val("operating")) | g | run
        self.assertEqual(value(c), "operating")

        d,e,f = (ET.Machine, RT.Kind, Val("operating")) | g | run
        self.assertEqual(now(c), now(f))

        self.assertEqual(d | Out[RT.Kind] | Ins[RT.Kind] | func[set] | collect,
                         {now(a), now(d)})

        self.assertEqual(g | now | all | length | collect, 7)

    def test_type_nodes(self):
        g = Graph()

        ae = AET.Type | g | run
        ae | assign[AET.Int] | g | run

        self.assertEqual(value(ae | now), AET.Int)

    def test_logic_type(self):
        g = Graph()

        even_type = Int & Is[modulo[2] | equals[0]]

        ae = AET[even_type] | g | run

        self.assertEqual(RAET_get_token(AET(ae)).complex_value.deserialize(), even_type)

        ae | assign[2] | g | run
        with self.assertRaises(Exception):
            ae | assign[3] | g | run
        ae | assign[4.0] | g | run

        self.assertEqual(type(value(ae | now)), float)
        self.assertEqual(type(value(ae | now | time_travel[-1])), int)

    def test_logic_type_hacked(self):
        import json
        from zef.pyzef.zefops import SerializedValue

        g = Graph()

        even_type = Int & Is[modulo[2] | equals[0]]
        s_typ = SerializedValue.serialize(even_type)

        ae = instantiate(internals.AttributeEntityType(s_typ), g)

        ae | assign[2] | g | run
        with self.assertRaises(Exception):
            ae | assign[3] | g | run
        ae | assign[4.0] | g | run

        self.assertEqual(type(value(ae | now)), float)
        self.assertEqual(type(value(ae | now | time_travel[-1])), int)

    def test_assign_value_node(self):
        g = Graph()

        ae = AET.Int | g | run

        vn = zef.pyzef.main.instantiate_value_node(2, g)
        vn2 = zef.pyzef.main.instantiate_value_node(3.0, g)

        ae | assign[vn] | g | run
        ae | assign[vn2] | g | run

        ae | assign[Val(5)] | g | run

        self.assertEqual(type(value(ae | now)), int)
        self.assertEqual(type(value(ae | now | time_travel[-1])), int)
        self.assertEqual(type(value(ae | now | time_travel[-2])), int)

        # There shouldn't be an extra value node after assigning the same value.
        g = Graph()
        num_before = g | all | filter[is_a[BT.VALUE_NODE]] | length | collect
        vn = Val({"something": 42}) | g | run
        ae = AET.Any | g | run
        ae | assign[Val({"something": 42})] | g | run
        num_after = g | all | filter[is_a[BT.VALUE_NODE]] | length | collect
        self.assertEqual(num_after, num_before + 2)

        # There shouldn't be an extra value node if the value is used as a value
        # and a value type
        g = Graph()
        num_before = g | all | filter[is_a[BT.VALUE_NODE]] | length | collect
        vn = Val(VT.Graph) | g | run
        ae = AET.Any | g | run
        ae2 = AET[VT.Graph] | g | run
        ae | assign[Val(VT.Graph)] | g | run
        num_after = g | all | filter[is_a[BT.VALUE_NODE]] | length | collect
        self.assertEqual(num_after - num_before, 2)

if __name__ == '__main__':
    unittest.main()