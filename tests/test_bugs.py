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

        r = GraphDelta([
            ET.Person["joe"],
            (Z["joe"], RT.FirstName["rel"], "name"),
        ]) | g | run

        self.assertEqual(1, g | now | all[ET.Person] | length | collect)
        self.assertEqual(1, g | now | all[RT.FirstName] | length | collect)

        r["rel"] | terminate | g | run
        self.assertEqual(1, g | now | all[ET.Person] | length | collect)
        self.assertEqual(0, g | now | all[RT.FirstName] | length | collect)

    def test_terminate_relation_merge(self):
        g = Graph()

        r = GraphDelta([
            ET.Person["joe"],
            (Z["joe"], RT.FirstName["rel"], "name"),
        ]) | g | run

        self.assertEqual(1, g | now | all[ET.Person] | length | collect)
        self.assertEqual(1, g | now | all[RT.FirstName] | length | collect)

        g2 = Graph()
        r2 = GraphDelta([
            r["rel"] | terminate["rel from g2"],
        ]) | g2 | run
        self.assertEqual(1, g | now | all[ET.Person] | length | collect)
        self.assertEqual(1, g | now | all[RT.FirstName] | length | collect)
        self.assertEqual(0, g2 | now | all[ET.Person] | length | collect)
        self.assertEqual(0, g2 | now | all[RT.FirstName] | length | collect)
        self.assertEqual(None, r2["rel from g2"])

    def test_no_duplicate_internal_name(self):
        g = Graph()
        with self.assertRaises(Exception):
            r = GraphDelta([
                ET.Danny["a"],
                ET.Machine["a"],
                (Z["a"], RT.Something, "name")
            ]) | g | run
        

if __name__ == '__main__':
    unittest.main()
