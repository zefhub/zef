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

        z = instantiate(ET.Machine, g) | fill_or_attach[RT.Weight, QuantityFloat(100.0, EN.Unit.kilogram)] | collect
        z2 = (instantiate(ET.Machine, g)
              | fill_or_attach[RT.Name, AET.String, "asdf"]
              | fill_or_attach[RT.Status, "asdf"]
              | collect)
        z3 = instantiate(ET.Machine, g) | fill_or_attach[[(RT.Number, 5),
                                                         (RT.Time, now()),
                                                         (RT.Fraction, 0.5),
                                                         (RT.Status, EN.Status.On),
                                                         (RT.Disabled, True)]] | collect

        self.assertEqual(length(z | out_rels[RT]), 1)
        self.assertEqual(length(z2 | out_rels[RT]), 2)
        self.assertEqual(length(z3 | out_rels[RT]), 5)


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

        ae | assign_value["word"] | g | run
        self.assertEqual(value(now(ae)), "word")

        g | now | all[AET] | single | assign_value["single"] | g | run
        self.assertEqual(value(now(ae)), "single")

if __name__ == '__main__':
    unittest.main()
