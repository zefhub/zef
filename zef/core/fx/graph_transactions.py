from .fx_types import Effect


def graph_transaction_handler(eff: Effect):
    """[summary]

    Args:
        eff (Effect): Effect({
            "type": FX.TX.Transact,
            "target_graph": g1,
            "graph_delta": some_g_delta
        })
    """
    # print(f"will transact {eff}")
    if eff.d["target_graph"].graph_data.is_primary_instance:
        from ..graph_delta import perform_transaction
        res = perform_transaction(eff.d['graph_delta'], eff.d['target_graph'])
    else:
        from ..overrides import merge
        res = merge(eff.d["graph_delta"], eff.d["target_graph"])
    
    # we need to forward this if it is in the effect
    if 'unpacking_template' in eff.d:
        res['unpacking_template'] = eff.d['unpacking_template']
    return res
