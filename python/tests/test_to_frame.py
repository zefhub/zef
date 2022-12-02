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
    def test_do_not_show_terminated_relents_in_Transaction(self):
        from zef.core.VT.rae_types import RAET_get_token
        g = Graph()
        with Transaction(g) as ctx: 
            m1 = instantiate(RAET_get_token(ET.Machine), g)
            m2 = instantiate(RAET_get_token(ET.Machine), g)
            m3 = instantiate(RAET_get_token(ET.Machine), g)
        terminate(m2) | g | run
        m4 = instantiate(RAET_get_token(ET.Machine), g)

        self.assertEqual(m4 | frame | collect, g | now | collect)
        ts4 = m4 | frame | graph_slice_index | collect

        with Transaction(g) as ctx: 
            this_frame = frame(ctx)
            self.assertEqual(this_frame, g | now | collect)
            self.assertEqual(ctx | to_graph_slice | graph_slice_index | collect, g | now | graph_slice_index | collect)
            self.assertEqual(ctx | to_graph_slice | graph_slice_index | collect, ts4+1)
            

        mm1 = m1 | to_ezefref | to_frame[g | now|collect] | collect
        mm1d = m1 | now | collect
        self.assertEqual(mm1, mm1d)
        
        self.assertEqual(m1 | now | frame | collect, g|now | collect)

        # self.assertEqual(ZefRefs([m1, m3]) | to_frame[now(g)] | frame | collect, g|now | collect)
        # TODO: Get to_frame working on ZefRefs again
        # self.assertEqual(ZefRefs([m1, m3]) | to_frame[g | now|collect] | frame | collect, g|now | collect)

        




if __name__ == '__main__':
    unittest.main()
