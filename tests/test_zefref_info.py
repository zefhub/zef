import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef.ops import *
from zef import *

class MyTestCase(unittest.TestCase):
    def test_info(self):
        g = Graph()

        instantiate(AET.String, g)
        instantiate(AET.String, g)
        f = instantiate(AET.String, g)

        w = instantiate(ET.Worker, g)

        assign_value(f, 'Fritz')
        assign_value(f, 'Ninja')

        instantiate(w, RT.YearOfBirth, f, g)
        assign_value(f, 'Ninja2')
        assign_value(f, 'Ninja')
        assign_value(f, 'Yolandi')


        man = instantiate(ET.Manager, g)
        m = instantiate(ET.Machine, g)
        m2 = instantiate(ET.Machine, g)

        m3 = instantiate(ET.Machine, g)
        n = instantiate(AET.String, g)


        instantiate(w, RT.CanOperate, m, g)
        instantiate(w, RT.Name, n, g)
        instantiate(w, RT.CanOperate, m2, g)
        rr = instantiate(w, RT.CanOperate, m3, g)
        instantiate(w, RT.CanOperate, m3, g)

        can_op = instantiate(man, RT.CanOperate, w, g)

        terminate(f) | g | run
        terminate(w) | g | run

        rr | to_frame[can_op | frame | collect] | yo[False] | collect
        rr | to_ezefref | yo[False] | collect

if __name__ == '__main__':
    unittest.main()
