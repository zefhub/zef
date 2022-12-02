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

from . import make_VT, insert_VT
from ... import pyzef

EZefRef = make_VT('EZefRef', pytype=pyzef.main.EZefRef)
ZefRef = make_VT('ZefRef', pytype=pyzef.main.ZefRef)
insert_VT("BlobPtr", ZefRef | EZefRef)
make_VT('GraphRef', pytype=pyzef.main.GraphRef)
make_VT('Time', pytype=pyzef.main.Time)

BaseUID = make_VT('BaseUID', pytype=pyzef.internals.BaseUID)
EternalUID = make_VT('EternalUID', pytype=pyzef.internals.EternalUID)
ZefRefUID = make_VT('ZefRefUID', pytype=pyzef.internals.ZefRefUID)
insert_VT("UID", (BaseUID | ZefRefUID | EternalUID))

make_VT('QuantityInt', pytype=pyzef.main.QuantityInt)
make_VT('QuantityFloat', pytype=pyzef.main.QuantityFloat)
make_VT('Enum', pytype=pyzef.main.ZefEnumValue)

def graph_ctor(*args, **kwargs):
    # Go to experimental load path for cases it supports
    # TODO: Uncomment this once event loop issue with IPython is figured out.
    # if len(kwargs) == 0 and len(args) == 1 and type(args[0]) == str:
    #     from ...experimental.repl_interface import load_graph
    #     return load_graph(args[0])
    # else:
    from ...core import internals
    return internals.Graph(*args, **kwargs)

Graph = make_VT('Graph', constructor_func=graph_ctor, pytype=pyzef.main.Graph)
insert_VT('DB', Graph)