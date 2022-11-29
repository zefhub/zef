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


    def test_dict_syntax(self):
        fg = FlatGraph([{
                ET.Person['z1'] : {
                RT.FirstName: "Fred",
                RT.YearOfBirth: 1970,
                RT.Parent: {
                    ET.Man: {
                        RT.Name: "Bob"
                    }
                }
            }
        }])

        g2 = Graph()
        fg | transact[g2] | run


    def test_mixed_types(self):
        fg = FlatGraph()
        fg = (fg 
        | insert[[
            ET.Foo['x1'],
            AET.Int | assign[41],
            AET.Int['k2'] | assign[41],
            (Any['x1'], RT.A['r2'], ET.Baz),
            (Any['r2'], RT.B, AET.String| assign["Fred"]),
            (Any['r2'], RT.C, AET.String['aet1'] | assign["Some"]),
            (Any['r2'], RT.G, "Egypt"),
            {ET.Person['p1']:{                             
            RT.Name: "Die Antwoord",
            RT.Style: "Rap-Rave",
            RT.Member['r1']: 5,}},
            (Any['p1'], RT.Name, "Ninja"),
        ]]
        | collect)


        g2 = Graph()
        fg | transact[g2] | run


    def test_all_types(self):
        g  = Graph()
        z0 = ET.Person | g | run
        z1 = EntityRef(z0)
        z2 = AttributeEntityRef(AET.Bool | g | run)
        rt = ((z0, RT.Name, AET.String) | g | run)[1]
        z3 = RelationRef(((rt, RT.Another, AET.String) | g | run)[1])
        z4 = AET.String | g | run
        z5 = (z4 | assign["hey"]) | g | run 

        fg = FlatGraph()
        fg = (fg 
        | insert[ET.Foo['x1']] 
        | insert[(AET.Int | assign[41])] 
        | insert[AET.Int['k2'] | assign[42] ] 
        | insert[(Any['x1'], RT.A['r2'], ET.Baz)]
        | insert[(Any['r2'], RT.B, AET.String| assign["Fred"])]    # assign value in one go
        | insert[(Any['r2'], RT.C, AET.String['aet1'] | assign["Some"])]    # internal id
        | insert[(Any['r2'], RT.G, "Egypt")]                  # value node
        | insert[AET.String] 
        | insert[AET.String['n1'] | assign["zeyad"]]
        | insert[AET.String | assign["wow"]]
        | insert[AET.Int['n2'] | assign[45]]
        | insert[Any['n2'] | assign[8]]
        | insert[ET.Person['p1']] 
        | insert[Val("r2")]
        | insert[Val(ET.Foo)]
        | insert[Val("English")]
        | insert[(ET.Cat['c1'], RT.D['r1'], Any['n1'])]
        | insert[(Any['r1'], RT.E, Val("English"))]
        | insert[(Any['r1'], RT.F, Any['r2'])]
        | insert[z4 | assign['500000'] ]
        | insert[z5]
        | insert[z1] 
        | insert[(z1, RT.H['t1'], Any['n1'])]
        | insert[RelationRef(rt)]
        | insert[z3]
        | insert[z2]
        | insert[z2 | assign[True]]
        | collect
        )

        # Transact Test
        g2 = Graph()
        fg | transact[g2] | run


    def test_fg_serialization_and_deserialization(self):
        fg = FlatGraph([{
                ET.Person['z1'] : {
                RT.FirstName: "Fred",
                RT.YearOfBirth: 1970,
                RT.Parent: {
                    ET.Man: {
                        RT.Name: "Bob"
                    }
                }
            }
        }])
        fg | to_json | from_json | collect 


    def test_fg_graphviz(self):
        fg = FlatGraph([{
                ET.Person['z1'] : {
                RT.FirstName: "Fred",
                RT.YearOfBirth: 1970,
                RT.Parent: {
                    ET.Man: {
                        RT.Name: "Bob"
                    }
                }
            }
        }])
        fg | graphviz | collect


    def test_merging_flatgraphs(self):
        g  = Graph()
        z0 = ET.Person | g | run
        
        fg = FlatGraph([{
                ET.Person['z1'] : {
                RT.FirstName: "Fred",
                RT.YearOfBirth: 1970,
                RT.Friend: z0,
                RT.Parent: {
                    ET.Man: {
                        RT.Name: "Bob"
                    }
                }
            }
        }])

        fg2 = FlatGraph()
        fg2 = (fg2 
            | insert[(ET.Foo['x1'], RT.X, z0)]       
            | insert[(ET.Cat, RT.Y['r1'],AET.String['n1'] |assign["42"])]    
            | insert[(Any['r1'], RT.Z, Val("English"))]   
            | insert[Val("Zeyad")]
            | insert[AET.Float['f1']] 
            | collect
        )

        merge(fg, fg2)


if __name__ == '__main__':
    unittest.main()