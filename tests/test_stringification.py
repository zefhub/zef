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
