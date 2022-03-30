import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
import os
os.environ["ZEF_DEVELOPER_LOCAL_TOKENS"] = "1"
os.environ["ZEFHUB_URL"] = "MASTER"
os.environ["ZEF_QUIET"] = "1"
from zef import *
from zef.ops import *
import zef


class MyTestCase(unittest.TestCase):

    def test_something(self):
        g = Graph()
        with Transaction(g):
            n1 = instantiate(ET.Person, g)
            n2 = instantiate(ET.Pet, g)
            r1 = instantiate(n1, RT.HasPet, n2, g)

        self.assertEqual(n1 >> RT.HasPet | collect, n2)
        self.assertEqual(n1 >> RT.HasPet << RT.HasPet | collect, n1)

        with Transaction(g):
            n3 = instantiate(ET.Person, g)
            r2 = instantiate(n1, RT.HasPet, n3, g)

        self.assertEqual(n1 >> RT.HasPet | collect, n2)

    def test_something2(self):
        g = Graph()
        with Transaction(g):
            c1 = instantiate(ET.Person, g)
            p1 = instantiate(ET.Pet, g)
            c2 = instantiate(ET.Person, g)
            p2 = instantiate(ET.Pet, g)
            r1 = instantiate(c1, RT.HasPet, p1, g)
            r2 = instantiate(c2, RT.HasPet, p2, g)

        v = ZefRefs([c1, c2]) >> RT.HasPet | collect
        self.assertEqual(len(v), 2)
        self.assertEqual(v[0], p1)
        self.assertEqual(v[1], p2)

        vv = ZefRefss([v, v, v, v])
        for x in vv:
            str(x)

    def test_find_source_tx(self):
        g = Graph()
        with Transaction(g):
            c1 = instantiate(ET.Person, g)
            p1 = instantiate(ET.Pet, g)
            c2 = instantiate(ET.Person, g)
            p2 = instantiate(ET.Pet, g)
            r1 = instantiate(c1, RT.HasPet, p1, g)
            r2 = instantiate(c2, RT.HasPet, p2, g)

        with Transaction(g):
            c3 = instantiate(ET.Person, g)
        with Transaction(g):
            terminate(c2) | g | run
            c4 = instantiate(ET.Person, g)
            p2 = instantiate(ET.Pet, g)
        with Transaction(g):
            c5 = instantiate(ET.Person, g)
        with Transaction(g):
            c6 = instantiate(ET.Person, g)

        zs = c1 | frame | all[ET.Person] | collect
        self.assertEqual(len(zs), 2)
        zs = g | now | all[ET.Person] | collect
        self.assertEqual(len(zs), 5)

    def test_assign_value(self):
        g = Graph()
        with Transaction(g):
            name = instantiate(AET.String, g)
            age =  instantiate(AET.Int, g)

        assign_value(name, 'Lu\0na\nis\nhungry') | g | run
        assign_value(age, 42) | g | run
        num = instantiate(AET.Int, g)
        assign_value(name, 'Naomi') | g | run
        assign_value(age, 43) | g | run

        name | terminate | g | run

        n2 = name | to_frame[num|frame|collect] | collect
        a2 = age | to_frame[num|frame|collect] | collect

        n2 | instantiation_tx | collect
        n2 | termination_tx | collect
        a2 | instantiation_tx | collect
        a2 | termination_tx | collect


    def test_terminate_selfloop(self):
        g = Graph()
        with Transaction(g):
            z = instantiate(ET.Machine, g)
            rel = instantiate(z, RT.TypeOf, z, g)

        terminate(z) | g | run

        self.assertEqual(length(g | now | all), 0)

        with Transaction(g):
            z = instantiate(ET.Machine, g)
            rel = instantiate(z, RT.TypeOf, z, g)

        terminate(ZefRefs([z,rel,z,z,z,rel,rel])) | g | run

        self.assertEqual(length(g | now | all), 0)

    def test_all_connected_relents(self):
        g = Graph()
        with Transaction(g):
            z = instantiate(ET.Machine, g)
            rel = instantiate(z, RT.TypeOf, z, g)
            rel2 = instantiate(rel, RT.Value, rel, g)

        zrs = ZefRefs([rel2])
        from zef.deprecated.tools import all_connected_entities
        self.assertEqual(set(all_connected_entities(zrs)), {z,rel,rel2})

    def test_no_external_graph_relations(self):
        g = Graph()
        g2 = Graph()
        z = instantiate(ET.Machine, g)
        with self.assertRaisesRegex(RuntimeError, "Not allowing an edge to be created between UZRs on a different graph"):
            instantiate(z, RT.TypeOf, z, g2)

    def test_O_class(self):
        g = Graph()
        with Transaction(g):
            z_zero = instantiate(ET.Machine, g)
            z_one,_,_ = (ET.Machine, RT.TypeOf, 1) | g | run
            z_two = ET.Machine | g | run
            (z_two, RT.TypeOf, 1) | g | run
            (z_two, RT.TypeOf, 2) | g | run

        self.assertEqual(z_zero >> O[RT.TypeOf] | collect, None)
        self.assertEqual(z_one >> O[RT.TypeOf] | collect, z_one >> RT.TypeOf | collect)
        with self.assertRaisesRegex(RuntimeError, "Unable to traverse"):
            z_two >> O[RT.TypeOf] | collect

if __name__ == '__main__':
    unittest.main()
