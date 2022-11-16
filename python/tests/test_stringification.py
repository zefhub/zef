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
        from zef.core.VT.rae_types import RAET_get_token
        from zef.ops import token_name

        et = instantiate(RAET_get_token(ET.Entity), g)
        rt = instantiate(et, RAET_get_token(RT.Relation), et, g)
        aet = instantiate(RAET_get_token(AET.String), g)

        self.assertEqual(str(BT(et)), 'BT.ENTITY_NODE')
        self.assertEqual(str(BT(aet)), 'BT.ATTRIBUTE_ENTITY_NODE')
        self.assertEqual(str(BT(rt)), 'BT.RELATION_EDGE')
        self.assertEqual(token_name(BT(et)), 'ENTITY_NODE')
        self.assertEqual(token_name(BT(aet)), 'ATTRIBUTE_ENTITY_NODE')
        self.assertEqual(token_name(BT(rt)), 'RELATION_EDGE')

        self.assertEqual(token_name(ET(et)), 'Entity')
        self.assertEqual(str(ET(et)), 'ET.Entity')

        self.assertEqual(token_name(AET(aet)), 'String')
        self.assertEqual(str(AET(aet)), 'AET.String')

        self.assertEqual(token_name(RT(rt)), 'Relation')
        self.assertEqual(str(RT(rt)), 'RT.Relation')

        aet2 = instantiate(RAET_get_token(AET.QuantityFloat.kilograms), g)
        self.assertEqual(str(AET(aet2)), 'AET.QuantityFloat.kilograms')
        self.assertEqual(token_name(AET(aet2)), 'QuantityFloat.kilograms')

        en = EN.Unit.kilograms
        self.assertEqual(repr(en), 'EN.Unit.kilograms')
        self.assertEqual(en.enum_type, "Unit")
        self.assertEqual(en.enum_value, "kilograms")


if __name__ == '__main__':
    unittest.main()
