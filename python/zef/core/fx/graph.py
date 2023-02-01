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

from .._ops import *
from ..VT import *

def graph_take_transactor_role_handler(eff: dict):
    from ...pyzef.main import make_primary
    make_primary(eff["graph"], True)
    return {}

def graph_release_transactor_role_handler(eff: dict):
    from ...pyzef.main import make_primary
    make_primary(eff["graph"], False)
    return {}

def graph_transaction_handler(eff: dict):
    """[summary]

    Args:
        eff (dict): {
            "type": FX.Graph.Transact,
            "target_graph": g1,
            "commands": list_of_commands
        }
    """


    if isinstance(eff["target_graph"], FlatGraph) or (isinstance(eff["target_graph"], GraphRef) and internals.is_transactor(eff["target_graph"])):
        # For backwards compatibility we dispatch on whether this is a new or old transact effect

        if "level2_commands" in eff:
            # This is the new path
            from ..graph_additions.wish_translation2 import generate_level1_commands
            if eff["translation_rules"] == "default":
                pass
            else:
                raise Exception("There are no options for translation rules at the moment. TODO: Need to replace with distinguish/recombine/relabel/cull rules later.")

            target_ref = eff["target_graph"]
            if isinstance(target_ref, GraphRef):
                target_ref = now(Graph(target_ref))
            lvl1_cmds = generate_level1_commands(eff["level2_commands"]["cmds"], target_ref)

            from ..graph_additions.low_level import perform_level1_commands
            receipt = perform_level1_commands(lvl1_cmds, eff.get("keep_internal_ids", False))

            # TODO: Postprocess using custom info stored in the level2 commands info
            post_transact_rule = eff.get("post_transact_rule", None)
            if post_transact_rule is not None:
                receipt = post_transact_rule(receipt, lvl2_custom=eff["level2_commands"].get("custom", None))
        else:
            from ..graph_delta import perform_transaction_commands
            receipt = perform_transaction_commands(eff['commands'], eff['target_graph'])
    else:
        from ..overrides import merge
        receipt = merge(eff["commands"], eff["target_graph"])
    
    # Handling receipt response needs a similar dispatch between old and new
    if "level2_commands" in eff:
        # New path
        from ..graph_additions.shorthand import unpack_receipt
        if 'unpacking_template' in eff:
            after_gs = now(Graph(eff["target_graph"]))
            return unpack_receipt(eff['unpacking_template'], receipt, after_gs)
        else:
            g = internals.get_loaded_graph(eff["target_graph"])
            if g is not None:
                # Since g is loaded, we can return ZefRefs instead of AtomRefs.
                gs = now(g)
                from .. import func
                receipt = (receipt
                           | items
                           | map[match[
                               (Is[second | is_a[AtomRef]], apply[first, lambda x: gs | get[second(x)] | collect]),
                               (Any, identity)
                               ]]
                           | func[dict]
                           | collect)
        return receipt
    else:
        from ..graph_delta import perform_transaction_commands, filter_temporary_ids, unpack_receipt
        # we need to forward this if it is in the effect
        if 'unpacking_template' in eff:
            return unpack_receipt(eff['unpacking_template'], receipt)
        receipt = filter_temporary_ids(receipt)
        return receipt
    

def graph_load_handler(eff: dict):
    # TODO: Make the underlying function be asynchronous and then update this to return an awaitable

    from ...pyzef.main import load_graph
    kwargs = {}
    if "mem_style" in eff:
        kwargs["mem_style"] = eff["mem_style"]
    if "progress_stream" in eff:
        kwargs["callback"] = lambda progress,stream=eff["progress_stream"]: progress | push[stream] | run
    if "progress_callback" in eff:
        kwargs["callback"] = eff["progress_callback"]

    g = load_graph(eff["tag_or_uid"], **kwargs)

    return {"g": g}