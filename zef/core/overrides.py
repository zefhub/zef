__all__ = [
    "merge",
]

from ..pyzef.main import Graph

# Override merge so that we may pass in a dict into the C++ code for GraphDeltas
def merge(obj, g : Graph, fire_and_forget : bool = False):
    from ..pyzef.main import merge as orig_merge
    from .serialization import serialize, deserialize

    # TODO Make sure this doesn't break something else!
    # The check below was done on GraphDelta. But for now the serialized behavior is spoofed.
    # My worry is the else statement. When is it hit because how can we be sure that obj is serialized.
    if type(obj) == list:
        serialized =  {"_zeftype": "GraphDelta", "value": serialize(list(obj))}
        rs =  orig_merge(serialized, g, fire_and_forget)
        receipt = deserialize(rs)
        return receipt
    else:
        return orig_merge(obj, g, fire_and_forget)

