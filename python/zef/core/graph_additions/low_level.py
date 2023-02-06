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
report_import("zef.core.graph_additions.low_level")

from ..VT import *
from .._ops import *
from ..internals import Transaction
from ... import pyzef
from ..VT.rae_types import RAET_get_token

from .common import *

def perform_level1_commands(command_struct: Level1CommandInfo, keep_internal_ids: Bool) -> GraphWishReceipt:
    assert isinstance(command_struct, Level1CommandInfo)

    if len(command_struct.cmds) == 0:
        # There is nothing to do! Let's just return early with the old graph slice
        gs_updated = command_struct.gs
        receipt = {}
        return gs_updated, receipt


    # Run through each command

    # For each command:
    # - resolve required ids
    # - action command
    # - insert into tracking dict the generated id

    gs = command_struct.gs
    g = Graph(gs)

    # The receipt is for items that are returned to the wisher
    receipt: GraphWishReceipt = {}
    # The mapping is a way for us to keep track of items created internally
    internal_mapping: Dict[InternalIDs | WrappedValue | DelegateRef][ZefRef] = {}

    # Optimised euid lookup, instead of looking up using full lineage system all the time
    euid_mapping: Dict[EternalUID][ZefRef] = {}
    def find_euid(euid: EternalUID):
        if euid in euid_mapping:
            return euid_mapping[euid]

        zr = most_recent_rae_on_graph(euid, g)
        euid_mapping[euid] = zr
        return zr

    def find_id(id: AtomRef | AllIDs) -> ZefRef:
        # print("find_id", id)
        if isinstance(id, EternalUID):
            return find_euid(id)
        elif isinstance(id, AtomRef):
            return find_euid(origin_uid(id))
        elif isinstance(id, InternalIDs):
            return internal_mapping[id]
        elif isinstance(id, WrappedValue | DelegateRef):
            if id in internal_mapping:
                return internal_mapping[id]
            # It must already be on the graph
            return now(g) | get[id] | collect
        else:
            assert isinstance(id, Variable)
            return find_euid(origin_uid(receipt[id]))

    def record_id(id: AllIDs, z: ZefRef):
        # print("record_id", id, z)
        if isinstance(id, InternalIDs | WrappedValue | DelegateRef):
            internal_mapping[id] = z
        elif isinstance(id, Variable):
            receipt[id] = Atom(z)
        else:
            raise Exception("Shouldn't get here")


    with Transaction(g) as ctx:
        # Note: we might have a transaction open around us so check both now and
        # the previous graph slices.
        if gs != now(g) and gs != now(g) | time_travel[-1] | collect:
            raise Exception("Can't perform level 1 commands onto a different graph slice from which they were constructed: {now(g)=}, {gs=}")

        for cmd in command_struct.cmds:
            # print("Doing cmd", cmd)
            if isinstance(cmd, PleaseInstantiate):
                # If is a brand-new item (no origin_uid) then create
                # Otherwise, merge in, referencing the original via lineage

                # z will be the object created/merged
                z = None
                if "origin_uid" in cmd:
                    assert isinstance(cmd.origin_uid, EternalUID)
                    assert cmd.origin_uid not in gs

                    if cmd.origin_uid in g:
                        # In this branch, the foreign RAE already exists and we
                        # need to link it back up. This could have happened with
                        # an instance that was created and terminated.
                        raise NotImplementedError("TODO: reviving a terminated instance")
                    else:
                        if isinstance(cmd["atom"], PleaseInstantiateEntity):
                            z = internals.merge_entity_(g, RAET_get_token(cmd.atom), cmd.origin_uid.blob_uid, cmd.origin_uid.graph_uid)
                            z = now(z)
                        elif isinstance(cmd["atom"], PleaseInstantiateAttributeEntity):
                            z = internals.merge_atomic_entity_(g, RAET_get_token(cmd.atom), cmd.origin_uid.blob_uid, cmd.origin_uid.graph_uid)
                            z = now(z)
                        elif isinstance(cmd["atom"], PleaseInstantiateRelation):
                            z_src = find_id(cmd["atom"]["source"])
                            z_trg = find_id(cmd["atom"]["target"])
                            z = internals.merge_relation_(g, RAET_get_token(cmd.atom["rt"]),
                                                          to_ezefref(z_src), to_ezefref(z_trg),
                                                          cmd.origin_uid.blob_uid, cmd.origin_uid.graph_uid)
                            z = now(z)
                        else:
                            raise NotImplementedError(f"TODO cmd.atom for merge: {cmd['atom']}")

                    euid_mapping[cmd.origin_uid] = z
                else:
                    if isinstance(cmd.atom, PleaseInstantiateEntity | PleaseInstantiateAttributeEntity):
                        raet = RAET_get_token(cmd.atom)
                        z = pyzef.main.instantiate(RAET_get_token(cmd.atom), g)
                    elif isinstance(cmd.atom, PleaseInstantiateRelation):
                        z_source = find_id(cmd.atom["source"])
                        z_target = find_id(cmd.atom["target"])
                        z = pyzef.main.instantiate(z_source, RAET_get_token(cmd.atom["rt"]), z_target, g)
                    elif isinstance(cmd.atom, PleaseInstantiateDelegate):
                        z = to_delegate(cmd.atom, now(g), True)
                        # Special case for recording an id
                        record_id(cmd.atom, z)
                    elif isinstance(cmd.atom, PleaseInstantiateValueNode):
                        val = internals.val_as_serialized_if_necessary(cmd.atom)
                        z = pyzef.main.instantiate_value_node(val, Graph(gs))
                        # Special case for recording an id
                        record_id(cmd.atom, z)
                    else:
                        raise NotImplementedError("TODO cmd.atom")

                # TODO: This should really be a "to ref" which would just be a
                # normal ZefRef in the future. For now, we'll do it this way,
                # but have the final processing of the receipt to turn these
                # into ZefRefs, just like the old graph delta.
                if "internal_ids" in cmd:
                    # print("About to record", z)
                    for id in cmd.internal_ids:
                        record_id(id, z)
            elif isinstance(cmd, PleaseAssign):
                z = find_id(cmd.target)
                internals.assign_value_imp(z, cmd.value.arg)
            elif isinstance(cmd, PleaseTerminate):
                z = find_id(cmd.target)
                pyzef.zefops.terminate(z)
            else:
                raise NotImplementedError(f"TODO cmd: {cmd}")

    # We keep track of the exact GraphSlice resulting from this transaction
    gs_updated = GraphSlice(ctx)

    # The receipt also gains the predetermined variables that were able to be
    # resovled before reaching this function.
    for k,alias in command_struct.resolved_variables.items():
        assert isinstance(k, Variable)
        record_id(k, find_id(alias))

    if keep_internal_ids:
        receipt.update(internal_mapping)

    # We undo any non-variable user ids based upon their included value
    receipt = maybe_unwrap_variables_in_receipt(receipt)
     
    return gs_updated, receipt
