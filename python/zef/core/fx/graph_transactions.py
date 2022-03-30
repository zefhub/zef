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

from .fx_types import Effect


def graph_transaction_handler(eff: Effect):
    """[summary]

    Args:
        eff (Effect): Effect({
            "type": FX.TX.Transact,
            "target_graph": g1,
            "commands": list_of_commands
        })
    """

    from ..graph_delta import perform_transaction_commands, filter_temporary_ids, unpack_receipt

    if eff.d["target_graph"].graph_data.is_primary_instance:
        receipt = perform_transaction_commands(eff.d['commands'], eff.d['target_graph'])
    else:
        from ..overrides import merge
        receipt = merge(eff.d["commands"], eff.d["target_graph"])
    
    # we need to forward this if it is in the effect
    if 'unpacking_template' in eff.d:
        return unpack_receipt(eff.d['unpacking_template'], receipt)

    receipt = filter_temporary_ids(receipt)
    return receipt