
def _graphdelta_merge(g, serialized_delta):
    """Don't call this explicitly. Only for the zefdb core."""

    from ..serialization import serialize, deserialize
    from .. import Graph, Effect, FX
    from .._ops import run, transact

    # Double check primary role here.
    # if g.
    # TODO The reason we are constructing an effect is that serialized 
    # data contains the commands and not the actions.
    # Can we somehow fix that?
    commands = deserialize(serialized_delta)
    # receipt = delta | transact[g] | run
    receipt = Effect({
            "type": FX.TX.Transact,
            "target_graph": g,
            "commands": commands
    }) | run

    return serialize(receipt)
