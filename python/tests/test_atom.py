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

    def test_atom(self):
        g = Graph()

        z = ET.Machine | g | run
        # TODO
        atom = g | now | all[ET.Machine] | single | collect
        atom = Atom(atom)

        self.assertIsInstance(atom, Atom)
        from zef.core.atom import get_ref_pointer
        self.assertEqual(get_ref_pointer(atom), z)
        self.assertEqual(get_ref_pointer(to_ezefref(atom)), to_ezefref(z))
        self.assertEqual(rae_type(atom), rae_type(z))
        # self.assertEqual(time(atom), time(z))
        self.assertEqual(preceding_events(atom), preceding_events(z))
        # Currently fails - behaviour should change
        with self.subTest():
            self.skipTest("uid behaviour needs to change")
            self.assertEqual(uid(atom), uid(z))
        self.assertEqual(exists_at(atom, now(g)), exists_at(z, now(g)))
        self.assertEqual(frame(atom), frame(z))
        self.assertEqual(discard_frame(atom), discard_frame(z))

        z2 = AET.Int | g | run
        zef.pyzef.zefops.assign_value(z2, 42)
        z2 = now(z2)
        atom2 = Atom(z2)
        self.assertEqual(value(atom2), value(z2))

        from zef.core.VT.rae_types import RAET_get_token
        rel = zef.pyzef.main.instantiate(to_ezefref(z), RAET_get_token(RT.Something), to_ezefref(z2), g)

        atom_rel = Atom(rel)

        self.assertEqual(get_ref_pointer(now(atom)), now(get_ref_pointer(atom)))

        z = now(z)
        atom = now(atom)
        z2 = now(z2)
        atom2 = now(atom2)

        self.assertEqual(source(atom_rel), atom)
        self.assertEqual(source(rel), z)
        self.assertEqual(target(atom_rel), atom2)
        self.assertEqual(target(rel), z2)

        self.assertEqual(Out(atom, RT.Something), atom2)
        self.assertEqual(out_rel(atom, RT.Something), atom_rel)
        self.assertEqual(Out(z, RT.Something), z2)
        self.assertEqual(out_rel(z, RT.Something), rel)

        self.assertEqual(In(z2, RT.Something), z)
        self.assertEqual(in_rel(z2, RT.Something), rel)

        self.assertEqual(has_out(atom, RT.Something), has_out(z, RT.Something))
        self.assertEqual(has_in(atom, RT.Something), has_in(z, RT.Something))

        self.assertEqual(get_ref_pointer(select_by_field([atom], RT.Something, 42)), select_by_field([z], RT.Something, 42))


if __name__ == '__main__':
    unittest.main()
