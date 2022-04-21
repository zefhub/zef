import os
os.environ["ZEFDB_DEVELOPER_EARLY_TOKENS"] = "1"
os.environ["ZEFDB_DEVELOPER_LOCAL_TOKENS"] = "1"
os.environ["ZEFHUB_URL"] = "MASTER"

from zef import *
from zef.ops import *
import zef.core.internals

g = Graph()
g2 = Graph()

d =[ET.User["z"]]
r = d | transact[g] | run

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

l = zef.core.internals.early_token_list()
with open("early.tokens", "w") as file:
    for s in l:
        print(s, file=file)

l = zef.core.internals.created_token_list()
with open("all_created.tokens", "w") as file:
    for s in l:
        print(s, file=file)
