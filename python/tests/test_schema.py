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

        z = ET.Machine | g | run

        self.assertEqual(g | blueprint | length | collect, 2)

        a,b,c = (z, RT.Something, 5) | g | run

        self.assertEqual(g | blueprint | length | collect, 5)

        self.assertEqual(g | now | blueprint | length | collect, 5)
        self.assertEqual(z | frame | blueprint | length | collect, 2)

        dz = delegate_of(z)
        db = delegate_of(b)

        with self.assertRaises(Exception):
            zef.pyzef.zefops.retire(to_ezefref(dz))
        with self.assertRaises(Exception):
            zef.pyzef.zefops.retire(to_ezefref(db))

        # Retire a node
        z | terminate | g | run

        zef.pyzef.zefops.retire(to_ezefref(db))

        self.assertEqual(g | now | blueprint | length | collect, 4)

        zef.pyzef.zefops.retire(to_ezefref(dz))
        self.assertEqual(g | now | blueprint | length | collect, 3)

    def test_meta(self):
        g = Graph()

        z = ET.Machine | g | run

        dz = to_ezefref(delegate_of(z))
        a,b,c = (dz, RT.Meta, 1) | g | run

        db = to_ezefref(delegate_of(b))

        with self.assertRaises(Exception):
            zef.pyzef.zefops.retire(dz)
        with self.assertRaises(Exception):
            zef.pyzef.zefops.retire(db)

        z | terminate | g | run

        with self.assertRaises(Exception):
            zef.pyzef.zefops.retire(dz)
        with self.assertRaises(Exception):
            zef.pyzef.zefops.retire(db)

        b | terminate | g | run

        with self.assertRaises(Exception):
            zef.pyzef.zefops.retire(dz)
        zef.pyzef.zefops.retire(db)

        with self.assertRaises(Exception):
            zef.pyzef.zefops.retire(dz)
        zef.pyzef.zefops.retire(delegate_of(dz))

        zef.pyzef.zefops.retire(dz)

    def test_recreate_delegate(self):
        g = Graph()

        z = ET.Machine | g | run

        self.assertEqual(g | now | blueprint | length | collect, 2)

        z | terminate | g | run
        zef.pyzef.zefops.retire(to_ezefref(delegate_of(z)))

        self.assertEqual(g | now | blueprint | length | collect, 1)

        z = ET.Machine | g | run

        self.assertEqual(g | now | blueprint | length | collect, 2)



if __name__ == '__main__':
    unittest.main()
