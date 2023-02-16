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

import os
os.environ["ZEFDB_DEVELOPER_EARLY_TOKENS"] = "1"
os.environ["ZEFDB_OFFLINE_MODE"] = "TRUE"
os.environ["ZEFDB_TOKENS_CACHE_PATH"] = ""

from zef import *
from zef.ops import *
import zef.core.internals

l = zef.core.internals.early_token_list()
with open("early.tokens", "w") as file:
    for s in l:
        print(s, file=file)


g = Graph()
g2 = Graph()

d =[ET.User["z"]]
_,r = d | transact[g] | run

z = r["z"]
d2 =[
    z,
    (ET.Token, RT.Index, AET.Int["aet"]),
]
d2 | transact[g2] | run

# g | subscribe[lambda x: None]
# r2["aet"] | subscribe[on_value_assignment][lambda x: None]


from zef import serialize, deserialize

s = serialize(d)
deserialize(s)

s = serialize(d2)
deserialize(s)

# TODO: Add more things in here that zefhub might have to do.

l = zef.core.internals.created_token_list()
with open("all_created.tokens", "w") as file:
    for s in l:
        print(s, file=file)
