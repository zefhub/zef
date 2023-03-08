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

from ... import report_import
report_import("zef.core.graph_additions.transact")

# This is the interface for the user

# Notes:
# - rules for interpretation (input -> lvl2 cmds) are acted upon in transact.
# - rules for transaltion (lvl2 -> lvl1) are acted upon in the effect.
# - lvl1 has no rules. It is the lowest-lvl commands that map 1-to-1 to the
#   operations applied to a graph.

# To better handle sending graph wishes over the wire, while zef functions are
# not serializable, we will use a string "default" for the translation rules.


from .common import *


@func
def transact_dispatch(input, onto, *others):
    # if isinstance(onto, (Graph, GraphRef)):
    if True:
        return transact_graph(input, onto, *others)
    elif isinstance(onto, FlatGraph):
        from ..op_implementations import fg_insert_imp
        return fg_insert_imp(onto, input)
    else:
        raise Exception(f"Don't understand kind of object to transact onto: {onto}")

@func
def transact_graph(input: GraphWishInput | List[GraphWishInput], g, interpretation_rules=None, translation_rules="default", post_transact_rule=None):
    # assert isinstance(input, GraphWishInput | List[GraphWishInput])
    # Some helpful output before we have proper "explain" options
    if not isinstance(input, GraphWishInput | List[GraphWishInput]):
        if isinstance(input, List):
            for x in input:
                if not isinstance(x, GraphWishInput):
                    raise TypeError(f"The following is not a valid GraphWishInput: {x}")
        raise TypeError(f"The following is not a valid GraphWishInput: {input}")
    if isinstance(input, GraphWishInput):
        input = [input]

    if interpretation_rules is None:
        from .wish_interpretation import default_interpretation_rules
        interpretation_rules = default_interpretation_rules
        
    from .wish_interpretation import generate_level2_commands
    cmds = generate_level2_commands(input, interpretation_rules)

    if isinstance(g, Graph | GraphRef):
        target_ref = GraphRef(g)
    elif isinstance(g, FlatGraph):
        target_ref = g
    else:
        raise Exception("Unknown target for a transaction")

    from ..fx import FX
    return {
        "type": FX.Graph.Transact,
        "target_glike": target_ref,
        "level2_commands": cmds,
        "translation_rules": translation_rules,
        "post_transact_rule": post_transact_rule,
    }
    
