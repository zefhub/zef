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


__all__ = [
    "config"
]

import os
import yaml
from ..core.op_implementations.dispatch_dictionary import _op_to_functions
from ..core import *
from ..core._ops import *
from ..pyzef import main as pymain

def get_config(key):
    return pymain.get_config_var(key)

def set_config(key, val):
    return pymain.set_config_var(key, val)

def list_config(filter):
    tuples = pymain.list_config(filter)

    d = {}
    for path,val in tuples:
        d = insert_in(d, path.split('.'), val)
    return d


def config_implementation(payload, action):
    if action == KW.set:
        assert isinstance(payload, tuple)
        assert len(payload) == 2
        assert isinstance(payload[0], str)
        set_config(payload[0], payload[1])
    elif action == KW.get:
        assert isinstance(payload, str)
        return get_config(payload)
    elif action == KW.list:
        assert isinstance(payload, str)
        return list_config(payload)
    else:
        raise NotImplementedError("Action {} not implemented".format(action))


def config_typeinfo(v_tp):
    return VT.Any


_op_to_functions[internals.RT.Config] = (config_implementation, config_typeinfo)

config = ZefOp(((internals.RT.Config, ()), ))
