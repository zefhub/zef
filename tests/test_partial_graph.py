import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *
import zef

class MyTestCase(unittest.TestCase):
    def test_zefrefs_from_slices(self):
        g = Graph()

        g_clones = []
        for i in range(100):
            g_clones += [zef.pyzef.internals.create_partial_graph(g, g.graph_data.write_head)]
            [ET.Machine]*10 | g | run

        for i,tx in enumerate(g | all[TX] | drop[1] | collect):
            g_clone_before_tx = g_clones[i]

            for j in range(i+1,len(g_clones)):
                g_partial = zef.pyzef.internals.create_partial_graph(g_clones[j], g_clone_before_tx.graph_data.write_head)
                self.assertEqual(g_clone_before_tx.hash(), g_partial.hash())

        

if __name__ == '__main__':
    unittest.main()
