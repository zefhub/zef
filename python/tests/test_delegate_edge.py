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

import zef
from zef import *
from zef.ops import *


class MyTestCase(unittest.TestCase):
    def test_rel_delegate_from_rel_source_or_target(self):
        g = Graph()

        I = lambda *args: instantiate(*args, g)
        with Transaction(g):
            m1 = I(ET.Machine)
            m2 = I(ET.Machine)
            r1 = I(m1, RT.EtoE, m2)
            rel_E_DR = I(m1, RT.EtoDR, r1 | delegate_of | collect)
        zef.pyzef.verification.verify_graph_double_linking(g)
        # serialize_check(g)




if __name__ == '__main__':
    unittest.main()
