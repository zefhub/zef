import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *


class MyTestCase(unittest.TestCase):
    def test_local_func(self):
        @func
        def adder(x, y=1):
            return x + y

        self.assertEqual(5 | adder | collect, 6)
        self.assertEqual(5 | adder[2] | collect, 7)

    def test_g_func(self):
        g = Graph()

        @func(g)
        def adder(x, y=1):
            return x + y

        self.assertEqual(5 | func[adder] | collect, 6)
        self.assertEqual(5 | func[adder][2] | collect, 7)

if __name__ == '__main__':
    unittest.main()
