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

# This file contains tests for things that were bugs at one point. Does not need
# to be organised at all

import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
import os
from zef import *
from zef.ops import *
import zef


class MyTestCase(unittest.TestCase):

    def test_terminate_relation(self):
        g = Graph()

        _,r = [
            ET.Person["joe"],
            (Z["joe"], RT.FirstName["rel"], "name"),
        ] | transact[g] | run

        self.assertEqual(1, g | now | all[ET.Person] | length | collect)
        self.assertEqual(1, g | now | all[RT.FirstName] | length | collect)

        r["rel"] | terminate | g | run
        self.assertEqual(1, g | now | all[ET.Person] | length | collect)
        self.assertEqual(0, g | now | all[RT.FirstName] | length | collect)

    def test_terminate_relation_merge(self):
        g = Graph()

        _,r = [
            ET.Person["joe"],
            (Z["joe"], RT.FirstName["rel"], "name"),
        ] | transact[g] | run

        self.assertEqual(1, g | now | all[ET.Person] | length | collect)
        self.assertEqual(1, g | now | all[RT.FirstName] | length | collect)

        g2 = Graph()
        _,r2 = [
            r["rel"] | terminate,
        ] | transact[g2] | run
        self.assertEqual(1, g | now | all[ET.Person] | length | collect)
        self.assertEqual(1, g | now | all[RT.FirstName] | length | collect)
        self.assertEqual(0, g2 | now | all[ET.Person] | length | collect)
        self.assertEqual(0, g2 | now | all[RT.FirstName] | length | collect)

    def test_no_duplicate_internal_name(self):
        g = Graph()
        with self.assertRaises(Exception):
            _,r = [
                ET.Danny["a"],
                ET.Machine["a"],
                (Z["a"], RT.Something, "name")
            ] | transact[g] | run

    def gil_locks(self):
        g = Graph()
        with self.assertRaisesRegex(Exception, "Can't lock a graph"):
            zef.pyzef.internals.test_pre_lock_hook(g)

if __name__ == '__main__':
    unittest.main()
