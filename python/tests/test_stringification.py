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

import unittest
from zef import *


class TestStringMethods(unittest.TestCase):

    def test_blobtype_name(self):
        g = Graph()
        et = instantiate(ET.Entity, g)
        rt = instantiate(et, RT.Relation, et, g)
        aet = instantiate(AET.String, g)

        self.assertEqual(str(BT(et)), 'ENTITY_NODE')
        self.assertEqual(str(BT(aet)), 'ATOMIC_ENTITY_NODE')
        self.assertEqual(str(BT(rt)), 'RELATION_EDGE')

        self.assertEqual(str(ET(et)), 'Entity')
        self.assertEqual(repr(ET(et)), 'ET.Entity')

        self.assertEqual(str(AET(aet)), 'String')
        self.assertEqual(repr(AET(aet)), 'AET.String')

        self.assertEqual(str(RT(rt)), 'Relation')
        self.assertEqual(repr(RT(rt)), 'RT.Relation')

        aet2 = instantiate(AET.QuantityFloat.kilograms, g)
        self.assertEqual(repr(AET(aet2)), 'AET.QuantityFloat.kilograms')
        self.assertEqual(str(AET(aet2)), 'QuantityFloat.kilograms')

        en = EN.Unit.kilograms
        self.assertEqual(repr(en), 'EN.Unit.kilograms')
        self.assertEqual(en.enum_type, "Unit")
        self.assertEqual(en.enum_value, "kilograms")


if __name__ == '__main__':
    unittest.main()
