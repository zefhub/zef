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

class MyTestCase(unittest.TestCase):

    def test_attach(self):
        g = Graph()

        z = ET.Machine | g | run
        z | fill_or_attach[RT.Weight][QuantityFloat(100.0, EN.Unit.kilogram)] | g | run
        z2 = ET.Machine | g | run
        z2 | fill_or_attach[RT.Name]["asdf"] | g | run
        z2 | fill_or_attach[RT.Status]["asdf"] | g | run
        z3 = ET.Machine | g | run
        [z3 | fill_or_attach[RT.Number][5] | collect,
         z3 | fill_or_attach[RT.Time][now()] | collect,
         z3 | fill_or_attach[RT.Fraction][0.5] | collect,
         z3 | fill_or_attach[RT.Status][EN.Status.On] | collect,
         z3 | fill_or_attach[RT.Disabled][True]
         ] | transact[g] | run

        self.assertEqual(length(z | now | out_rels[RT]), 1)
        self.assertEqual(length(z2 | now | out_rels[RT]), 2)
        self.assertEqual(length(z3 | now | out_rels[RT]), 5)

    def test_set_field(self):
        g = Graph()

        z = ET.Machine | g | run
        z | set_field[RT.Weight][QuantityFloat(100.0, EN.Unit.kilogram)] | g | run
        z2 = ET.Machine | g | run
        z2 | set_field[RT.Name]["asdf"] | g | run
        z2 | set_field[RT.Status]["asdf"] | g | run
        z3 = ET.Machine | g | run
        [z3 | set_field[RT.Number][5] | collect,
         z3 | set_field[RT.Time][now()] | collect,
         z3 | set_field[RT.Fraction][0.5] | collect,
         z3 | set_field[RT.Status][EN.Status.On] | collect,
         z3 | set_field[RT.Disabled][True]
         ] | transact[g] | run

        self.assertEqual(length(z | now | out_rels[RT]), 1)
        self.assertEqual(length(z2 | now | out_rels[RT]), 2)
        self.assertEqual(length(z3 | now | out_rels[RT]), 5)

        r = [ET.Machine["z"],
             ET.Person["y"],
             Z["z"] | set_field[RT.Supervisor][Z["y"]],
             ] | transact[g] | run
        self.assertEqual(r["z"] | Out[RT.Supervisor] | collect, r["y"])
        
        r2 = [ET.Machine["z"] | set_field[RT.Supervisor][ET.Person["y"]]] | transact[g] | run
        self.assertEqual(r2["z"] | Out[RT.Supervisor] | collect, r2["y"])

        r3 = [r["z"] | set_field[RT.Supervisor][ET.Person["new"]]] | transact[g] | run
        self.assertEqual(r["z"] | now | Outs[RT.Supervisor] | length | collect, 1)
        self.assertEqual(r["z"] | now | Out[RT.Supervisor] | collect, r3["new"])


    def test_set_field_reversed(self):
        g = Graph()

        z = ET.Machine | g | run
        z | set_field[RT.Something][42][True] | g | run

        self.assertEqual(z | now | in_rels[RT.Something] | length | collect, 1)
        self.assertEqual(z | now | In[RT.Something] | value | collect, 42)

        z | set_field[RT.Something][43][True] | g | run

        self.assertEqual(z | now | In[RT.Something] | value | collect, 43)

        z2 = ET.Machine | g | run
        z | set_field[RT.Entity][z2][True] | g | run

        self.assertEqual(z | now | In[RT.Entity] | collect, now(z2))

        z3 = ET.Machine | g | run
        z | set_field[RT.Entity][z3][True] | g | run
        self.assertEqual(z | now | In[RT.Entity] | collect, now(z3))


    def test_shorthand(self):
        g = Graph()

        z = ET.Dog | g | run
        self.assertTrue(is_a(z, ET.Dog))

        z1, [(name_rel, name),
             (breed_rel, breed)] = (z, [(RT.Name, "Rufus"),
                                        (RT.Breed, "Labrador")]) | g | run

        self.assertEqual(to_ezefref(z), to_ezefref(z1))
        self.assertTrue(is_a(name_rel, RT.Name))
        self.assertEqual(value(name), "Rufus")
        self.assertTrue(is_a(breed_rel, RT.Breed))
        self.assertEqual(value(breed), "Labrador")

        z1, none, [a,b,c] = (z, RT.Nickname, ["Dog", "Boy", "Mongrel"]) | g | run

        self.assertEqual(to_ezefref(z), to_ezefref(z1))
        self.assertEqual(none, None)
        self.assertEqual(value(a), "Dog")
        self.assertEqual(value(b), "Boy")
        self.assertEqual(value(c), "Mongrel")
        self.assertTrue(has_relation(now(z), RT.Nickname, a))
        self.assertTrue(has_relation(now(z), RT.Nickname, b))
        self.assertTrue(has_relation(now(z), RT.Nickname, c))

        [a,b], none, z1 = (["Alice", "Bob"], RT.Owner, z) | g | run

        self.assertEqual(to_ezefref(z), to_ezefref(z1))
        self.assertEqual(none, None)
        self.assertEqual(value(a), "Alice")
        self.assertEqual(value(b), "Bob")
        self.assertTrue(has_relation(a, RT.Owner, now(z)))
        self.assertTrue(has_relation(b, RT.Owner, now(z)))

    def test_shorthand_assign(self):
        g = Graph()

        ae = AET.String | g | run
        self.assertEqual(value(ae), None)

        ae | assign["word"] | g | run
        self.assertEqual(value(now(ae)), "word")

        g | now | all[AET] | single | assign["single"] | g | run
        self.assertEqual(value(now(ae)), "single")

if __name__ == '__main__':
    unittest.main()
