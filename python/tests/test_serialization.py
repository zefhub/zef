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
    def test_serialization(self):
        from zef.core.graph_delta import PleaseAssign

        g = Graph()
        z = ET.Machine | g | run
        a,b,c = (z, RT.Something, "data") | g | run

        to_check = [z,a,b,c]
        to_check += [discard_frame(a)]
        to_check += [discard_frame(b)]
        to_check += [discard_frame(c)]
        to_check += [uid(z)]
        to_check += [PleaseAssign(target=discard_frame(c), value=5)]

        to_check += [
            {"key": 5,
             z: ([z,z,(z,z),[z,z], {z: z}], z)
             }
        ]

        for item in to_check:
            self.assertEqual(deserialize(serialize(item)), item)

if __name__ == '__main__':
    unittest.main()
