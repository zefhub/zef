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

from ...pyzef.zefops import (
    L,
    O,
    Q,
    affected,
    allow_terminated_relent_promotion,
    assert_length,
    batch,
    concatenate,
    delegate,
    element,
    exists_at,
    filter,
    first,
    flatten,
    has_in,
    has_out,
    has_relation,
    incoming,
    ins,
    ins_and_outs,
    instances,
    instances_eternal,
    instantiated,
    instantiation_tx,
    intersect,
    is_zefref_promotable,
    is_zefref_promotable_and_exists_at,
    keep_alive,
    last,
    lift,
    not_in,
    now,
    on_instantiation,
    on_termination,
    on_value_assignment,
    only,
    only_or,
    outgoing,
    outs,
    relation,
    set_union,
    sort,
    source,
    subscribe,
    take,
    target,
    terminate,
    terminated,
    termination_tx,
    time,
    time_slice,
    time_travel,
    to_frame,
    to_ezefref,
    tx,
    uid,
    unique,
    value,
    value_assigned,
    value_assignment_txs,
    without
)

from .attach import attach, fill_or_attach # this zefop is defined in python
make_field = attach # deprecation

# Need to override the filter behaviour because typing is hard in python
# from ..zef_functions import to_clipboard
# from ..graph_delta import transact

from ... import pyzef, core
pyzef.zefops.TimeZefopStruct.__getitem__ = lambda self, s: core.Time(s)


# This implementation is the imperative side and was moved from lazy_zefops
def _assign_value(z, value):
    from ..core import AET
    from ..ops import is_a
    from ..pyzef.zefops import assign_value, SerializedValue
    if is_a(z, AET.Serialized):
        from ..serialization import serialize, serializable_types
        if type(z) in serializable_types:
            from json import dumps
            value = SerializedValue("tools.serialize", dumps(serialize(value)))
        else:
            raise Exception(f"Don't know how to serialize a type of {val}")
    return assign_value(z, value)