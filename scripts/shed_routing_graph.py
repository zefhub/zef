
# Notes: to shed the graph, need to run this script to create the "shedded" graph and then copy it over on S3 while ZefHub is shutdown. Steps:
# - python shed_routing_graph.py
# - get UIDs of routing_graph and shed_routing_graph
# - shutdown zefhub
# - S3 cp the routing_guid.json file to local
# - modify UID
# - S3 cp local file back up to S3 routing_graph
#
# - Note: there may be an error with "number of candidates is 0" but I think
#   this is just the graph being unsubscribed to as an update comes in (which is
#   very regular with the routing_graph)


from zefdb import *
from zefdb.zefops import *


g = Graph("routing_graph")

zrs = all_current_relents(g)

g2 = Graph()
duplicate(zrs, g2, True)

del(g)
sync(g2, True)
tag(g2, "shed_routing_graph")
