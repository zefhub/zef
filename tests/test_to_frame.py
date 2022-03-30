import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *


class MyTestCase(unittest.TestCase):
    def test_do_not_show_terminated_relents_in_Transaction(self):
        g = Graph()
        with Transaction(g) as ctx: 
            m1 = instantiate(ET.Machine, g)
            m2 = instantiate(ET.Machine, g)
            m3 = instantiate(ET.Machine, g)
        terminate(m2) | g | run
        m4 = instantiate(ET.Machine, g)

        self.assertEqual(m4 | frame | collect, g | now | collect)
        ts4 = m4 | frame | time_slice | collect

        with Transaction(g) as ctx: 
            this_frame = frame(ctx)
            self.assertEqual(this_frame, g | now | collect)
            self.assertEqual(ctx | time_slice | collect, g | now | time_slice | collect)
            self.assertEqual(ctx | time_slice | collect, ts4+1)
            

        mm1 = m1 | to_ezefref | to_frame[g | now|collect] | collect
        mm1d = m1 | now | collect
        self.assertEqual(mm1, mm1d)
        
        self.assertEqual(ZefRefs([m1, m3]) | now | frame | collect, g|now | collect)
        self.assertEqual(m1 | now | frame | collect, g|now | collect)

        # self.assertEqual(ZefRefs([m1, m3]) | to_frame[now(g)] | frame | collect, g|now | collect)
        # TODO: Get to_frame working on ZefRefs again
        # self.assertEqual(ZefRefs([m1, m3]) | to_frame[g | now|collect] | frame | collect, g|now | collect)

        




if __name__ == '__main__':
    unittest.main()
