from .fx_types import Effect

def graph_take_transactor_role_handler(eff: Effect):
    from ...pyzef.main import make_primary
    make_primary(eff.d["graph"], True)
    return {}

def graph_release_transactor_role_handler(eff: Effect):
    from ...pyzef.main import make_primary
    make_primary(eff.d["graph"], False)
    return {}