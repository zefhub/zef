
def _graphdelta_merge(g, serialized_delta):
    """Don't call this explicitly. Only for the zefdb core."""

    from ..serialization import serialize, deserialize
    from .. import Graph
    from .._ops import run

    # Double check primary role here.
    # if g.
    delta = deserialize(serialized_delta)

    receipt = delta | g | run

    return serialize(receipt)
