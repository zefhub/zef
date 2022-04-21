from .fx_types import Effect


def graph_transaction_handler(eff: Effect):
    """[summary]

    Args:
        eff (Effect): Effect({
            "type": FX.TX.Transact,
            "target_graph": g1,
            "commands": list_of_commands
        })
    """

    from ..graph_delta_new import perform_transaction_commands, filter_temporary_ids, unpack_receipt

    if eff.d["target_graph"].graph_data.is_primary_instance:
        receipt = perform_transaction_commands(eff.d['commands'], eff.d['target_graph'])

    # if eff.d["target_graph"].graph_data.is_primary_instance:
    #     receipt = perform_transaction(eff.d['graph_delta'], eff.d['target_graph'])
    # else:
    #     from ..overrides import merge
    #     receipt = merge(eff.d["graph_delta"], eff.d["target_graph"])
    
    # we need to forward this if it is in the effect
    if 'unpacking_template' in eff.d:
        return unpack_receipt(eff.d['unpacking_template'], receipt)

    receipt = filter_temporary_ids(receipt)
    return receipt