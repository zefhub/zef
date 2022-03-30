import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *

class MyTestCase(unittest.TestCase):
    def test_zefrefs_from_slices(self):

        g = Graph()
        z = AET.String | g | run
        (z <= "a") | g | run
        (z <= "b") | g | run
        z | terminate | g | run

        g2 = Graph()
        z2 = z | g2 | run

        ctx = z | instantiated | collect
        # Note: slice 1 is created on Graph(), so this is slice 2
        self.assertEqual(ctx | time_slice | collect, 2)

        self.assertEqual(ctx | instantiated | length | collect, 1)
        self.assertEqual(ctx | value_assigned | length | collect, 0)
        self.assertEqual(ctx | terminated | length | collect, 0)
        self.assertEqual(ctx | merged | length | collect, 0)

        txs = z | now[allow_tombstone] | value_assigned | collect
        self.assertEqual([ctx | time_slice | collect for ctx in txs], [3,4])
        for ctx in txs:
            self.assertEqual(ctx | instantiated | length | collect, 0)
            self.assertEqual(ctx | value_assigned | length | collect, 1)
            self.assertEqual(ctx | terminated | length | collect, 0)
            self.assertEqual(ctx | merged | length | collect, 0)
            

        ctx = z | terminated | collect
        self.assertEqual(ctx | time_slice | collect, 5)

        self.assertEqual(ctx | instantiated | length | collect, 0)
        self.assertEqual(ctx | value_assigned | length | collect, 0)
        self.assertEqual(ctx | terminated | length | collect, 1)
        self.assertEqual(ctx | merged | length | collect, 0)

        ctx = z2 | terminated | collect
        self.assertEqual(ctx, None)

        ctx = z2 | merged | collect
        # Note: slice 1 is created on Graph(), so this is slice 2
        self.assertEqual(ctx | time_slice | collect, 2)

        self.assertEqual(ctx | instantiated | length | collect, 1)
        self.assertEqual(ctx | value_assigned | length | collect, 0)
        self.assertEqual(ctx | terminated | length | collect, 0)
        self.assertEqual(ctx | merged | length | collect, 1)

if __name__ == '__main__':
    unittest.main()
