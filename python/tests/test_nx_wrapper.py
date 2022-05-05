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
    def test_networkx(self):
        from networkx import DiGraph
        import networkx as nx
        # from networkx.algorithms.components import strongly_connected_components

        from zef.experimental.networkx import ProxyGraph

        g = Graph()
        zs = [ET.Machine]*42 | g | run
        for i,j in [(0,1), (1,2), (2,5), (5,10), (10,0),
                    (5,6), (6,7), (7,8), (8,6)]:
            (zs[i], RT.UsedBy, zs[j]) | g | run

        r = [
            (ET.Person["alex"], [(RT.FirstName, "Alex"),
                                (RT.Status, EN.Status.Developer),
                                (RT.ZefAge, 1)]),
            (ET.Person["bob"], [(RT.FirstName, "Bob"),
                                (RT.Status, EN.Status.Zefer),
                                (RT.ZefAge, 0)]),
            (ET.Person["zach"], [(RT.FirstName, "Zach"),
                                (RT.Status, EN.Status.Nobody)]),

            (Z["alex"], RT.Knows["d-e"], Z["bob"]),
            (Z["d-e"], RT.From, "University"),

            (Z["alex"], RT.Knows["d-z"], Z["zach"]),
            (Z["d-z"], RT.From, "Nowhere"),
        ] | transact[g] | run


        dg = ProxyGraph(now(g), ET.Person, RT.Knows)
        self.assertEqual(set(dg.nodes) | map[lambda x: x.z] | func[set] | collect,
                         set(g | now | all[ET.Person] | collect))
        self.assertEqual(set(dg.edges) | map[map[lambda x: x.z] | func[tuple]] | func[set] | collect,
                         set([(r["alex"], r["bob"]),
                              (r["alex"], r["zach"])]))
        self.assertEqual(dg[r["alex"]][r["bob"]]["From"], "University")
        self.assertEqual(dg.nodes[r["alex"]]["ZefAge"], 1)
        self.assertEqual(dg.edges[r["d-e"]]["From"], "University")

        dg2 = ProxyGraph(now(g), ET.Machine, RT.UsedBy)
        list(nx.strongly_connected_components(dg2))

        ug = ProxyGraph(now(g), ET.Person, RT.Knows, undirected=True)
        list(nx.connected_components(ug))

        nx.all_pairs_node_connectivity(ug)
        nx.all_pairs_node_connectivity(dg)

        nx.node_connectivity(ug)
        nx.node_connectivity(dg)

        nx.node_connectivity(ug, list(dg.nodes)[1], list(dg.nodes)[2])
        nx.node_connectivity(dg, list(dg.nodes)[1], list(dg.nodes)[2])

        last_two = [r["bob"], r["zach"]]
        dg_sg = dg.subgraph(last_two)
        ug_sg = ug.subgraph(last_two)
        self.assertEqual(nx.node_connectivity(ug), 1)
        self.assertEqual(nx.node_connectivity(ug_sg), 0)

        nx.k_components(ug)
        nx.k_components(dg2.to_undirected())

        # Can't work - tries to construct a graph itself
        # nx.maximum_branching(dg)
        nx.maximum_branching(dg.to_native())

        # Doesn't work - tries to use _pred
        # nx.average_clustering(dg)
        nx.average_clustering(dg.to_native())

        nx.diameter(ug)

        (r["bob"], RT.Knows, r["zach"]) | g | run
        ug_complete = ProxyGraph(now(g), ET.Person, RT.Knows, undirected=True)

        # Doesn't work - tries to construct a graph itself
        # nx.minimum_spanning_tree(ug_complete)
        nx.minimum_spanning_tree(ug.to_native())

        nx.degree_centrality(dg)
        nx.degree_centrality(ug)

        list(nx.chain_decomposition(ug_complete))

        nx.is_chordal(dg2.to_undirected())

        list(nx.enumerate_all_cliques(ug))

        nx.greedy_color(dg)
        nx.greedy_color(dg2)
        nx.greedy_color(ug)

        for k in range(2,4):
            list(nx.algorithms.community.k_clique_communities(dg2.to_undirected(), k))

        nx.shortest_path(ug)
        nx.shortest_path(dg)
        nx.shortest_path(dg2)

if __name__ == '__main__':
    unittest.main()