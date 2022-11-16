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

import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *
import zef

from zef.core.VT.rae_types import RAET_get_token

class MyTestCase(unittest.TestCase):

    def test_something(self):
        g = Graph()
        with Transaction(g):
            n1 = instantiate(RAET_get_token(ET.Person), g)
            n2 = instantiate(RAET_get_token(ET.Pet), g)
            r1 = instantiate(n1, RAET_get_token(RT.HasPet), n2, g)

        self.assertEqual(n1 | Out[RT.HasPet] | collect, n2)
        self.assertEqual(n1 | Out[RT.HasPet] | In[RT.HasPet] | collect, n1)

        with Transaction(g):
            n3 = instantiate(RAET_get_token(ET.Person), g)
            r2 = instantiate(n1, RAET_get_token(RT.HasPet), n3, g)

        self.assertEqual(n1 | Out[RT.HasPet] | collect, n2)

    def test_something2(self):
        g = Graph()
        with Transaction(g):
            c1 = instantiate(RAET_get_token(ET.Person), g)
            p1 = instantiate(RAET_get_token(ET.Pet), g)
            c2 = instantiate(RAET_get_token(ET.Person), g)
            p2 = instantiate(RAET_get_token(ET.Pet), g)
            r1 = instantiate(c1, RAET_get_token(RT.HasPet), p1, g)
            r2 = instantiate(c2, RAET_get_token(RT.HasPet), p2, g)

        v = [c1, c2] | map[Out[RT.HasPet]] | collect
        self.assertEqual(len(v), 2)
        self.assertEqual(v[0], p1)
        self.assertEqual(v[1], p2)

        # vv = ZefRefss([v, v, v, v])
        # for x in vv:
        #     str(x)

    def test_find_source_tx(self):
        g = Graph()
        with Transaction(g):
            c1 = instantiate(RAET_get_token(ET.Person), g)
            p1 = instantiate(RAET_get_token(ET.Pet), g)
            c2 = instantiate(RAET_get_token(ET.Person), g)
            p2 = instantiate(RAET_get_token(ET.Pet), g)
            r1 = instantiate(c1, RAET_get_token(RT.HasPet), p1, g)
            r2 = instantiate(c2, RAET_get_token(RT.HasPet), p2, g)

        with Transaction(g):
            c3 = instantiate(RAET_get_token(ET.Person), g)
        with Transaction(g):
            terminate(c2) | g | run
            c4 = instantiate(RAET_get_token(ET.Person), g)
            p2 = instantiate(RAET_get_token(ET.Pet), g)
        with Transaction(g):
            c5 = instantiate(RAET_get_token(ET.Person), g)
        with Transaction(g):
            c6 = instantiate(RAET_get_token(ET.Person), g)

        zs = c1 | frame | all[ET.Person] | collect
        self.assertEqual(len(zs), 2)
        zs = g | now | all[ET.Person] | collect
        self.assertEqual(len(zs), 5)

    def test_assign(self):
        g = Graph()
        with Transaction(g):
            name = instantiate(RAET_get_token(AET.String), g)
            age =  instantiate(RAET_get_token(AET.Int), g)

        assign(name, 'Lu\0na\nis\nhungry') | g | run
        assign(age, 42) | g | run
        num = instantiate(RAET_get_token(AET.Int), g)
        assign(name, 'Naomi') | g | run
        assign(age, 43) | g | run

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
            z = instantiate(RAET_get_token(ET.Machine), g)
            rel = instantiate(z, RAET_get_token(RT.TypeOf), z, g)

        terminate(z) | g | run

        self.assertEqual(length(g | now | all), 0)

        with Transaction(g):
            z = instantiate(RAET_get_token(ET.Machine), g)
            rel = instantiate(z, RAET_get_token(RT.TypeOf), z, g)

        [z,rel,z,z,z,rel,rel] | map[terminate] | g | run

        self.assertEqual(length(g | now | all), 0)

    def test_no_external_graph_relations(self):
        g = Graph()
        g2 = Graph()
        z = instantiate(RAET_get_token(ET.Machine), g)
        # with self.assertRaisesRegex(RuntimeError, "Not allowing an edge to be created between UZRs on a different graph"):
        with self.assertRaises(Exception):
            instantiate(z, RAET_get_token(RT.TypeOf), z, g2)

    def test_O_class(self):
        g = Graph()
        with Transaction(g):
            z_zero = instantiate(RAET_get_token(ET.Machine), g)
            z_one,_,_ = (ET.Machine, RT.TypeOf, 1) | g | run
            z_two = ET.Machine | g | run
            (z_two, RT.TypeOf, 1) | g | run
            (z_two, RT.TypeOf, 2) | g | run

        self.assertEqual(z_zero | Outs[RT.TypeOf] | single_or[None] | collect, None)
        self.assertEqual(z_one | Outs[RT.TypeOf] | single_or[None] | collect, z_one | Out[RT.TypeOf] | collect)
        # with self.assertRaisesRegex(Exception, "single_or detected more than one item in iterator"):
        with self.assertRaises(Exception):
            z_two | Outs[RT.TypeOf] | single_or[None] | collect

if __name__ == '__main__':
    unittest.main()
