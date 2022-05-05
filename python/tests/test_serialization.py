import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *


class MyTestCase(unittest.TestCase):
    def test_serialization(self):
        g = Graph()
        z = ET.Machine | g | run
        a,b,c = (z, RT.Something, "data") | g | run

        to_check = [z,a,b,c]
        to_check += [origin_rae(a)]
        to_check += [origin_rae(b)]
        to_check += [origin_rae(c)]
        to_check += [uid(z)]

        to_check += [
            {"key": 5,
             z: ([z,z,(z,z),[z,z], {z: z}], z)
             }
        ]

        for item in to_check:
            self.assertEqual(deserialize(serialize(item)), item)

if __name__ == '__main__':
    unittest.main()
