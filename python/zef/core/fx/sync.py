from .fx_types import Effect

def graph_sync_handler(eff: Effect):
    from ...pyzef.main import sync
    sync(eff.d["graph"], eff.d["sync_state"])
    return {}
