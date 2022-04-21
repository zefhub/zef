__all__ = [
    "merge",
]

from ..pyzef.main import Graph

# Override merge so that we may pass in a dict into the C++ code for GraphDeltas
def merge(obj, g : Graph, fire_and_forget : bool = False):
    from ..pyzef.main import merge as orig_merge
    from .serialization import serialize, deserialize

    raise NotImplementedError(f"THIS NEEDS TO BE FIXED!")
    # TODO Fix this to not depend on deprecated GraphDelta
    # if type(obj) == GraphDelta:
    #     ds = serialize(obj)
    #     rs = orig_merge(ds, g, fire_and_forget)
    #     receipt = deserialize(rs)
    #     return receipt
    # else:
    #     return orig_merge(obj, g, fire_and_forget)
