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
    def test_zef_enum_value(self):
        g = Graph()
        with Transaction(g):
            enum_type = "Zeyad"
            enum_val = "TEST"
            EN(enum_type, enum_val)
            ae = AET.Enum(enum_type) | g | run
            (ae <= EN(enum_type, enum_val)) | g | run

        retrieved_value = ae | value | collect
        self.assertEqual(retrieved_value.enum_value, enum_val)
        self.assertEqual(retrieved_value.enum_type, enum_type)


if __name__ == '__main__':
    unittest.main()
