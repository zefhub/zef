__all__ = [
    "merge",
]

from ..pyzef.main import Graph

# Override merge so that we may pass in a dict into the C++ code for GraphDeltas
def merge(obj, g : Graph, fire_and_forget : bool = False):
    from ..pyzef.main import merge as orig_merge
    from .serialization import serialize, deserialize

    serialized = serialize(tuple(obj))
    rs =  orig_merge(serialized, g, fire_and_forget)
    receipt = deserialize(rs)
    return receipt

