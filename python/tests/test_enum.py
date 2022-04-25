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
