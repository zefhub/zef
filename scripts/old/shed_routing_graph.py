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
