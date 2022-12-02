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

from zef.pyzef.zefops import on_value_assignment, subscribe, keep_alive, on_instantiation, on_termination, outgoing

class MyTestCase(unittest.TestCase):

    def test_subscribe(self):
        from zef.core.VT.rae_types import RAET_get_token
        g = Graph()
        z = instantiate(RAET_get_token(AET.Int), g)

        on_value_assign_list = []
        def OnValueAssign(x, l):
            l += [x|value|collect]

        sub1 = z | subscribe[on_value_assignment][keep_alive[False]][lambda x: OnValueAssign(x, on_value_assign_list)]

        on_rel_list = []

        def OnInst(x, l):
            l += [("inst", RT(x))]
        def OnTerm(x, l):
            l += [("term", RT(x))]
        sub2 = z | subscribe[on_instantiation[outgoing][RAET_get_token(RT.Map)]][keep_alive[False]][lambda x: OnInst(x, on_rel_list)]
        sub3 = z | subscribe[on_instantiation[outgoing][RAET_get_token(RT.Value)]][lambda x: OnInst(x, on_rel_list)]
        sub4 = z | subscribe[on_termination[outgoing][RAET_get_token(RT.Map)]][lambda x: OnTerm(x, on_rel_list)]
        sub5 = z | subscribe[on_termination[outgoing][RAET_get_token(RT.Value)]][lambda x: OnTerm(x, on_rel_list)]

        z | assign[1] | g | run
        z | assign[2] | g | run
        z | assign[3] | g | run
        z | assign[4] | g | run
        z | assign[5] | g | run

        (z, RT.Map, 1) | g | run
        (z, RT.Map, 2) | g | run
        (z, RT.Value, 2) | g | run

        z | now | Outs[RT.Map] | map[terminate] | g | run

        (z, RT.Value, 3) | g | run
        (z, RT.Map, 4) | g | run

        z | now | Outs[RT.Value] | map[terminate] | g | run

        # Test that keep_alive kills subs when it is False only
        sub2 = None
        sub3 = None
        import gc
        gc.collect()

        (z, RT.Map, 10) | g | run
        (z, RT.Value, 10) | g | run

        # Test that unsubscribe kills subs definitely
        sub4.unsubscribe()
        z | now | Outs[RT.Map] | map[terminate] | g | run

        self.assertEqual(on_value_assign_list, [1,2,3,4,5])
        self.assertEqual(on_rel_list, [
            ("inst", RT.Map),
            ("inst", RT.Map),
            ("inst", RT.Value),
            ("term", RT.Map),
            ("term", RT.Map),
            ("inst", RT.Value),
            ("inst", RT.Map),
            ("term", RT.Value),
            ("term", RT.Value),
            ("inst", RT.Value),
        ])
                          
if __name__ == '__main__':
    unittest.main()

