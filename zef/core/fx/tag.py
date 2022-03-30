from .fx_types import Effect
from ..error import Error

def graph_tag_handler(eff: Effect):
    from ...pyzef.main import tag
    tag(eff.d["graph"], eff.d["tag"], adding=eff.d["adding"], force=eff.d["force"])
    return {}


def rae_tag_handler(eff: Effect):
    from ...pyzef.main import tag
    if not eff.d["adding"]:
        return Error("Untagging a RAE is not supported (yet).")
    tag(eff.d["rae"], eff.d["tag"], force_if_name_tags_other_rel_ent=eff.d["force"])
    return {}
