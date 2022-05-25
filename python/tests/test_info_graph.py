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

def SetupGraph():
    print()
    g = Graph()
    with Transaction(g):
        for k in range(13456):
            instantiate(ET.Machine, g)
    with Transaction(g):
        instantiate(ET.ProcessOrder, g)
        instantiate(ET.Machine, g)
        m = instantiate(ET.Machine, g)

        scr = instantiate(ET.ZEF_Script, g)
        s = instantiate(AET.String, g)
        instantiate(scr, RT.ZEF_Python, s, g)

    return g,m,scr,s
    
class MyTestCase(unittest.TestCase):
    def test_return_types(self):
        g,m,scr,s = SetupGraph()
        
        m | now | yo[False] | collect
        g | yo[False] | collect

    def test_nested(self):
        g = Graph()

        z = ET.Machine | g | run
        a,b,c = (z, RT.Something, 5) | g | run
        d,e,f = (b, RT.Nested, z) | g | run

        g | yo[False] | collect
        
if __name__ == '__main__':
    unittest.main()
