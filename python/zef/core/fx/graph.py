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

    from ..graph_delta import perform_transaction_commands, filter_temporary_ids, unpack_receipt

    if eff["target_graph"].graph_data.is_primary_instance:
        receipt = perform_transaction_commands(eff['commands'], eff['target_graph'])
    else:
        from ..overrides import merge
        receipt = merge(eff["commands"], eff["target_graph"])
    
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