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
