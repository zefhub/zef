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


class MyTestCase(unittest.TestCase):

    def test_basics(self):
        g = Graph()

        r = [
            ET.Person["joe"],
            (Z["joe"], RT.FirstName, "Joe"),
            (Z["joe"], RT.LastName, "Bloggs"),
            (Z["joe"], RT.NickName, "Guy"),
            (Z["joe"], RT.NickName, "Joey"),

            ET.Pet["spot"],
            (Z["spot"], RT.Name, "Spot"),
            (Z["joe"], RT.HasPet["joe-spot"], Z["spot"]),

            (Z["joe-spot"], RT.Date, Time("12:34 May 2020")),
        ] | transact[g] | run

        self.assertEqual(r["joe"], g | now | all[ET.Person] | only | collect)
        self.assertEqual(r["spot"], g | now | all[ET.Pet] | only | collect)

        self.assertEqual(r["joe"] | Out[RT.FirstName] | value | collect, "Joe")
        self.assertEqual(r["joe"] | Out[RT.HasPet] | collect, r["spot"])
        self.assertEqual(r["joe"] | Outs[RT.NickName] | length | collect, 2)

        z_joe = r["joe"]
        r2 = [
            (r["joe"], RT.NickName, "Jay"),
            r["joe"] | Out[RT.LastName] | assign["Smith"],
        ] | transact[g] | run

        z2_joe = now(z_joe)

        self.assertEqual(z2_joe | Out[RT.LastName] | value | collect, "Smith")
        self.assertEqual(z2_joe | Outs[RT.NickName] | length | collect, 3)

        r3 = [
            *[x | terminate for x in z2_joe | Outs[RT.NickName] | collect],
        ] | transact[g] | run

        z3_joe = now(z2_joe)

        self.assertEqual(z3_joe | Outs[RT.NickName] | length | collect, 0)
        self.assertFalse(z3_joe | has_out[RT.NickName] | collect)
        

    def test_merges(self):
        g = Graph()

        r = [
            ET.Person["joe"],
            (Z["joe"], RT.FirstName, "Joe"),
            (Z["joe"], RT.LastName, "Bloggs"),
        ] | transact[g] | run

        z_joe = r["joe"]

        g2 = Graph()

        r2 = [
            z_joe,
            (z_joe, RT.Owns, ET.Account["acc"]),
            (Z["acc"], RT.Balance, QuantityFloat(100, EN.Unit.dollars)),
        ] | transact[g2] | run

        self.assertTrue(uid(z_joe|to_ezefref) in g2)
        self.assertEqual(length(g | now | all[ET.Person]), 1)

        z2_joe = g2 | now | get[origin_uid(z_joe)] | collect
        self.assertEqual(origin_uid(z_joe), origin_uid(z2_joe))
        # Check low-level structure
        self.assertEqual(uid(g2[origin_uid(z_joe)]), origin_uid(z_joe))
        self.assertEqual(z_joe | g2 | run, g2 | now | get[origin_uid(z_joe)] | collect)

        r3 = [
            (z2_joe, RT.FromMerge, True)
        ] | transact[g] | run

        self.assertEqual(length(g | now | all[ET.Person]), 1)
        self.assertEqual(z_joe | now | Out[RT.FromMerge] | value | collect, True)

        z3_joe = z_joe | g2 | run

        self.assertEqual(to_ezefref(z2_joe), to_ezefref(z3_joe))

    def test_multiple_relation_notation(self):
        g = Graph()

        r = [
            ([ET.Person["joe"], ET.Person["john"]], RT.Owns, [ET.Pet["cat"], ET.Pet["dog"]]),
        ] | transact[g] | run

        self.assertEqual(g | now | all[RT.Owns] | length | collect, 4)
        self.assertTrue(has_relation(r["joe"], RT.Owns, r["cat"]))
        self.assertTrue(has_relation(r["joe"], RT.Owns, r["dog"]))
        self.assertTrue(has_relation(r["john"], RT.Owns, r["cat"]))
        self.assertTrue(has_relation(r["john"], RT.Owns, r["dog"]))

        g = Graph()
        r = [
            (ET.Person, RT.Owns, [ET.Pet["a"], ET.Pet["b"]]),
        ] | transact[g] | run

        self.assertEqual(g | now | all[RT.Owns] | length | collect, 2)
        self.assertTrue(has_relation(g | now | all[ET.Person] | single | collect, RT.Owns, r["a"]))
        self.assertTrue(has_relation(g | now | all[ET.Person] | single | collect, RT.Owns, r["b"]))

        g = Graph()
        r = [
            (ET.Person["joe"], [(RT.FirstName, "Joe"),
                                (RT.LastName, "Bloggs")])
        ] | transact[g] | run

        self.assertEqual(r["joe"] | Out[RT.FirstName] | value | collect, "Joe")
        self.assertEqual(r["joe"] | Out[RT.LastName] | value | collect, "Bloggs")

    def test_relation_relations(self):
        g = Graph()
        z = ET.Example | g | run
        x, y, z = (z, RT.Something, 3) | g | run
        x, y, z = (y, RT.Something, 3) | g | run
        g | now | all[ET.Example] | last | out_rel[RT.Something] | Out[RT.Something] | collect

    def test_tagging(self):
        g = Graph()

        z = ET.Person | g | run
        r = [
            ET.Person["joe"],
            Z["joe"] | tag["secret"],
            z | tag["first"]
        ] | transact[g] | run

        self.assertEqual(r["joe"], g | now | get["secret"] | collect)
        self.assertEqual(z | now | collect, g | now | get["first"] | collect)

    def test_dictionary(self):
        g = Graph()

        y = ET.Person | g | run
        z = {ET.Person: {
            RT.Name: "Joe",
            RT.Supervisor: y
        }} | g | run

        self.assertEqual(z | Out[RT.Supervisor] | discard_frame | collect,
                         y | discard_frame | collect)
        self.assertEqual(z | F.Name | collect, "Joe")

    def test_object_notation(self):
        g = Graph()

        x = ET.Person(
            first_name="Joe",
            last_name="Bloggs",
            height=99,
            # friend=ET.Person(
            friend_temp=ET.Person(
                first_name="Jane",
                last_name="Doe",
                height=142,
            )
        )

        z_joe = x | g | run
        self.assertEqual(rae_type(z_joe), ET.Person)
        self.assertEqual(z_joe | F.first_name | collect, "Joe")
        self.assertEqual(z_joe | F.last_name | collect, "Bloggs")
        self.assertEqual(z_joe | F.height | collect, 99)
        # z_jane = z_joe | F.Friend | collect
        z_jane = z_joe | F.friend_temp | collect
        self.assertEqual(rae_type(z_jane), ET.Person)
        self.assertEqual(z_jane | F.first_name | collect, "Jane")
        self.assertEqual(z_jane | F.last_name | collect, "Doe")
        self.assertEqual(z_jane | F.height | collect, 142)

        g = Graph()

        y = ET.Person["joe"](
            first_name="Joe",
            last_name="Bloggs",
            height=99,
            # friend=ET.Person["jane"](
            friend_temp=ET.Person["jane"](
                first_name="Jane",
                last_name="Doe",
                height=142,
            )
        )

        r = [
            y,
            (Z["jane"], RT.Something, Z["joe"]),
        ] | transact[g] | run

        z_joe = r["joe"]
        self.assertEqual(rae_type(z_joe), ET.Person)
        self.assertEqual(z_joe | F.first_name | collect, "Joe")
        self.assertEqual(z_joe | F.last_name | collect, "Bloggs")
        self.assertEqual(z_joe | F.height | collect, 99)

        z_jane = r["jane"]
        # self.assertEqual(z_joe | F.friend | collect, z_jane)
        self.assertEqual(z_joe | F.friend_temp | collect, z_jane)
        self.assertEqual(rae_type(z_jane), ET.Person)
        self.assertEqual(z_jane | F.first_name | collect, "Jane")
        self.assertEqual(z_jane | F.last_name | collect, "Doe")
        self.assertEqual(z_jane | F.height | collect, 142)

        self.assertEqual(z_jane | Out[RT.Something] | collect, z_joe)

    def test_delegate_creation(self):
        g = Graph()

        d,r,s = (delegate_of(ET.Person), RT.Alias, "PersonEntity") | g | run

        z = ET.Person | g | run

        self.assertEqual(delegate_of(z) | to_delegate | collect, delegate_of(ET.Person))
        self.assertEqual(value(s), "PersonEntity")
        self.assertEqual(to_ezefref(delegate_of(z)), to_ezefref(d))
        self.assertEqual(rae_type(r), RT.Alias)
        self.assertEqual(source(r), d)
        self.assertEqual(target(r), s)
        # TODO: Redo this when we know how it should look.
        # self.assertEqual(isinstance(delegate_of(z), delegate_of(ET.Person)))

    def test_full_graph_merge(self):
        g = Graph()

        z = ET.Machine | g | run
        a,b,c = (ET.Machine, RT.Something, 5) | g | run
        d,e,f = (delegate_of(ET.Machine), RT.Metadata, Val("asdf")) | g | run
        _,h,i = (b, RT.Something, EN.Enum.Test) | g | run
        _,j,_ = (a, RT.Cycle, a) | g | run
        v = Val({"dict": "test"}) | g | run

        g2 = Graph()
        g | now | all | g2 | run

        for obj in [a,b,c,  e,  h,i,j]:
            self.assertIn(origin_uid(obj), g2)
            self.assertIn(discard_frame(obj), g2)
            z2 = g2 | now | get[discard_frame(obj)] | collect
            self.assertEqual(discard_frame(obj), discard_frame(z2))

        self.assertIsNot(internals.search_value_node(value(f), g2), None)
        self.assertIsNot(internals.search_value_node(internals.SerializedValue.serialize(value(v)), g2), None)
            
        
            

if __name__ == '__main__':
    unittest.main()
