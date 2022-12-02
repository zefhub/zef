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
from zef.ops import *
from zef import *

class MyTestCase(unittest.TestCase):
    def test_info(self):
        from zef.core.VT.rae_types import RAET_get_token

        g = Graph()

        instantiate(RAET_get_token(AET.String), g)
        instantiate(RAET_get_token(AET.String), g)
        f = instantiate(RAET_get_token(AET.String), g)

        w = instantiate(RAET_get_token(ET.Worker), g)

        assign(f, 'Fritz')
        assign(f, 'Ninja')

        instantiate(w, RAET_get_token(RT.YearOfBirth), f, g)
        assign(f, 'Ninja2')
        assign(f, 'Ninja')
        assign(f, 'Yolandi')


        man = instantiate(RAET_get_token(ET.Manager), g)
        m = instantiate(RAET_get_token(ET.Machine), g)
        m2 = instantiate(RAET_get_token(ET.Machine), g)

        m3 = instantiate(RAET_get_token(ET.Machine), g)
        n = instantiate(RAET_get_token(AET.String), g)


        instantiate(w, RAET_get_token(RT.CanOperate), m, g)
        instantiate(w, RAET_get_token(RT.Name), n, g)
        instantiate(w, RAET_get_token(RT.CanOperate), m2, g)
        rr = instantiate(w, RAET_get_token(RT.CanOperate), m3, g)
        instantiate(w, RAET_get_token(RT.CanOperate), m3, g)

        can_op = instantiate(man, RAET_get_token(RT.CanOperate), w, g)

        terminate(f) | g | run
        terminate(w) | g | run

        rr | to_frame[can_op | frame | collect] | yo[False] | collect
        rr | to_ezefref | yo[False] | collect

if __name__ == '__main__':
    unittest.main()
