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

from zef.core.graph_additions.types import *
from zef.core.graph_additions.low_level import *
from zef.core.graph_additions.wish_translation2 import *
from zef.core.graph_additions.wish_interpretation import *


class MyTestCase(unittest.TestCase):

    def test_level1(self):
        g = Graph()
        g2 = Graph()

        cmd = PleaseInstantiate({"atom": ET.Machine,
                                 "internal_ids": [V.asdf]})
        cmds = [cmd]
        concrete_cmd_list = Level1CommandInfo({"cmds": cmds,
                                               "gs": now(g),
                                               "resolved_variables": {}})
        gs_updated,receipt = perform_level1_commands(concrete_cmd_list, False)

        self.assertSetEqual(set(receipt.keys()), {V.asdf})
        self.assertIsInstance(receipt[V.asdf], ET.Machine)

        cmd = PleaseInstantiate({"atom": ET.Machine,
                                 "internal_ids": [V.two],
                                 "origin_uid": origin_uid(receipt[V.asdf])})
        cmds = [cmd]
        concrete_cmd_list = Level1CommandInfo({"cmds": cmds,
                                               "gs": now(g2),
                                               "resolved_variables": {}})
        gs_updated,receipt2 = perform_level1_commands(concrete_cmd_list, False)
        self.assertEqual(origin_uid(receipt[V.asdf]), origin_uid(receipt2[V.two]))

    def test_generate_level1(self):
        # lvl1_rules = default_translation_rules

        g = Graph()
        z = ET.Machine | g | run
        g2 = Graph()
        
        cmds = [PleaseInstantiate({"atom": ET.Machine,
                                   "internal_ids": [V.one]}),
                PleaseInstantiate({"atom": ET.Machine,
                                   "internal_ids": [V.two]}),
                PleaseInstantiate({"atom": ET.Machine,
                                   "origin_uid": origin_uid(z),
                                   "internal_ids": [V.three]}),
                PleaseInstantiate({"atom": ET.Machine,
                                   "origin_uid": origin_uid(z),
                                   "internal_ids": [V.four]})]

        output_cmds = generate_level1_commands(cmds, now(g))#, lvl1_rules)
        self.assertEqual(output_cmds.gs, now(g))
        self.assertSetEqual(set(output_cmds.cmds),
                         {PleaseInstantiate({"atom": ET.Machine,
                                             "internal_ids": [V.one]}),
                          PleaseInstantiate({"atom": ET.Machine,
                                             "internal_ids": [V.two]})})
        output_cmds2 = generate_level1_commands(cmds, now(g2))#, lvl1_rules)
        self.assertSetEqual(set(output_cmds2.cmds),
                         {PleaseInstantiate({"atom": ET.Machine,
                                             "internal_ids": [V.one]}),
                          PleaseInstantiate({"atom": ET.Machine,
                                             "internal_ids": [V.two]}),
                          PleaseInstantiate({"atom": ET.Machine,
                                             "origin_uid": origin_uid(z)})})

        gs_updated,receipt = perform_level1_commands(output_cmds2, False)
        self.assertEqual(set(receipt.keys()), {V.one, V.two, V.three, V.four})
        self.assertEqual(receipt[V.three], receipt[V.four])
        self.assertNotEqual(receipt[V.one], receipt[V.two])
        self.assertNotEqual(receipt[V.one], receipt[V.three])
        self.assertNotEqual(receipt[V.two], receipt[V.three])

        cmds = [PleaseTerminate({"target": origin_uid(z)}),
                PleaseInstantiate({"atom": ET.Machine,
                                   "internal_ids": [V.thisframe]}),
                PleaseTerminate({"target": V.thisframe})]
        with self.assertRaisesRegex(Exception, "Can't have two commands for the same name"):
            output_cmds3 = generate_level1_commands(cmds, now(g))#, lvl1_rules)

    def test_generate_level2(self):
        # lvl1_rules = default_translation_rules
        lvl2_rules = default_interpretation_rules

        g = Graph()
        z = ET.Machine | g | run
        g2 = Graph()
        
        info = generate_level2_commands([ET.Machine[V.one]], lvl2_rules)
        temp = PleaseInstantiate({"atom": ET.Machine, "internal_ids": [V.one]})
        self.assertEqual(info["cmds"], [PleaseInstantiate({"atom": ET.Machine,
                                                   "internal_ids": (V.one,)})])
        output_cmds = generate_level1_commands(info["cmds"], now(g))#, lvl1_rules)
        receipt = perform_level1_commands(output_cmds, False)


        info = generate_level2_commands([
            ET.Machine[V.one],
            AET.Int[V.other],
            (V.one, RT.Something[V.two], V.other),
        ], lvl2_rules)

        # TODO:
        # self.assertEqual(info, {"cmds": [
        #     PleaseInstantiate({"atom": ET.Machine,
        #                        "internal_ids": [V.one]}),
        #     PleaseInstantiate({"atom": AET.Int,
        #                        "internal_ids": [V.other]}),
        #     PleaseInstantiate({"atom": {"rt": RT.Something,
        #                                 "source": V.one,
        #                                 "target": V.other,
        #                                 "combine_source": False,
        #                                 "combine_target": False},
        #                        "internal_ids": [V.two]}),
        # ]})

        info = generate_level2_commands([
            ET.Machine[V.one](something=5),
            (V.one, RT.extra, V.one),
        ], lvl2_rules)
        cmds = info["cmds"]

        # TODO:
        # self.assertEqual(cmds[0], PleaseInstantiate({"atom": ET.Machine,
        #                                              "internal_ids": [V.one]}))
        # print(cmds)
        # print("==============")
        # print("==============")
        # print("==============")
        # print("==============")
        # print(cmds[1])
        # self.assertEqual(cmds[1].atom, AET.Int)
        # self.assertEqual([cmds[2].target], cmds[1].internal_ids)
        # self.assertEqual(cmds[2].value, Val(5))
        # self.assertEqual(cmds[3], PleaseInstantiate({"atom": {"rt": RT.something,
        #                                                       "source": V.one,
        #                                                       "target": cmds[1].internal_ids[0],
        #                                                       "combine_source": False,
        #                                                       "combine_target": False,}}))
        # self.assertEqual(cmds[4], PleaseInstantiate({"atom": {"rt": RT.extra,
        #                                                       "source": V.one,
        #                                                       "target": V.one,
        #                                                       "combine_source": False,
        #                                                       "combine_target": False}}))
        # self.assertEqual(len(cmds), 5)

        lvl1_cmds = generate_level1_commands(cmds, now(g))#, lvl1_rules)
        receipt = perform_level1_commands(lvl1_cmds, False)

    def test_obj_notation(self):
        g = Graph()

        z = ET.Location | g | run
        ae = AET.Int | g | run
        # loc = ET.Location(z)
        loc = ET.Location("㏈-51ec2f5e07f6063c1eb21d36ef2ccf19")

        inputs = [
            ET.Person[V.bob](name="Bob", favourite_food={"cheese", "pizza"}),
            ET.Person[V.jane](name="Jane", favourite_food={"pasta", "steak"}),

            (V.bob, RT.MarriedTo(V.married, meta="meta-test"), V.jane),

            V.married | set_field[RT.Date][Time("1999-01-01")],
            (V.married, RT.Location, [z, loc]),

            # loc.name << "seafront",
            # loc.country << "antartica",
            loc(people_visited={V.bob,V.jane}),

            ae | assign[42],
        ]


        from zef.core.graph_additions.types import GraphWishInput
        for input in inputs:
            self.assertIsInstance(input, GraphWishInput)

        gs_updated,receipt = inputs | transact[g] | run

        married_rel = receipt[V.married]
        z_married_rel = gs_updated | get[married_rel] | collect
        self.assertEqual(z_married_rel | F.meta | collect, "meta-test")
        self.assertEqual(value(now(ae)), 42)



        z_no_prior = ET.Machine | g | run
        z_prior = ET.Machine(something=42) | g | run
        # TODO: Things with sets here
        [
            Atom(z_no_prior)(something=43),
            Atom(z_prior)(something=43),
        ] | g | run
        self.assertEqual(z_no_prior | now | F.something | collect, 43)
        self.assertEqual(z_prior | now | F.something | collect, 43)


    def test_backwards_compatibility(self):
        g = Graph()

        z = ET.Location | g | run
        z2 = {ET.Location: {RT.Details: 0}} | g | run

        inputs = [
            {ET.Person["person"]: {RT.FirstName["a"]: "Joe", RT.LastName["b"]: "Bloggs"}},
            {Z["person"]: {RT.Location: "Antartica"}},
            (Z["person"], RT.Weird, Z["a"]),
            {Z["a"]: {RT.MetaData: "stuff"}},

            (Z["b"], RT.MetaData["meta"], 42),
            (Z["meta"], RT.Something["meta2"], 42),
            
            z | set_field[RT.Something][42],
            # z2 | Out[RT.Details] | assign[1],
        ]

        gs_updated,receipt = inputs | transact[g] | run


    def test_all_encode(self):
        # This is meant to follow the list that is in `shorthand.py`. It mostly
        # just checks to see if the cmds run, rather than their returns.
        g = Graph()

        z = ET.Machine(something=42) | g | run
        a,b,c = (z, RT.Something, 42) | g | run
        a,_,cs = (z, RT.Relation, [ET.Machine, ET.Machine]) | g | run
        delegate_of(ET.Machine) | g | run
        c | assign[43] | g | run
        z | g | run
        z | terminate | g | run
        {ET.Machine: {RT.Something: 42}} | g | run
        Val(42) | g | run

    def test_all_interpret(self):
        # This is meant to follow the list that is in `wish_interpretation.py`.
        # It mostly just checks to see if the cmds run, rather than their
        # returns.
        g = Graph()

        z = ET.Machine | g | run
        z2 = ET.Machine | g | run
        z_ae = AET.Int | g | run

        gs_updated,r = [
            ET.Machine(V.x, something=42),
            (V.x, RT.Something(V.relation, meta=1), 42),
            (z, RT.Relation, [ET.Machine, ET.Machine]),
            delegate_of(ET.Machine),
            z_ae | assign[43],
            z,
            z2 | terminate,
            {ET.Machine: {RT.Something: 42}},
            Val(42),
            ET.Machine[V.random],
            AET.Int[V.random2],
        ] | transact[g] | run

        # The collected variants
        z2 = ET.Machine | g | run
        z_ae = AET.Int | g | run

        gs_updated,r = [
            z_ae | assign[43] | collect,
            z2 | terminate | collect,
        ] | transact[g] | run

        with self.assertRaises(Exception):
            [
                ET.Machine(V.x),
                ET.Other(V.x),
            ] | transact[g] | run

        with self.assertRaises(Exception):
            [
                (ET.Machine, RT.Something(V.x), 42),
                ET.Other(V.x),
            ] | transact[g] | run

        with self.assertRaises(Exception):
            z = ET.Machine | g | run
            g2 = Graph()
            [
                z,
                z | terminate,
            ] | transact[g2] | run

        with self.assertRaises(Exception):
            [
                ET.Machine[V.something],
                V.something | terminate,
            ] | transact[g] | run
            

        


if __name__ == '__main__':
    unittest.main()
