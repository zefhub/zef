__all__ = [
    "grant",
    "revoke",
    "login",
    "logout",
]

from ..core import *
from ..core._ops import *

def privileges_implementation(user, action, privilege, target):
    # This just creates the effect, the actual implementation of the privileges
    # is in the fx module.

    if privilege == append:
        privilege = KW.append
        
    opts = {
        (KW.grant, KW.view): FX.Privileges.GrantView,
        (KW.grant, KW.append): FX.Privileges.GrantAppend,
        (KW.grant, KW.host): FX.Privileges.GrantHost,
        (KW.grant, KW.discover): FX.Privileges.GrantDiscover,
        (KW.grant, KW.modify_rights): FX.Privileges.GrantModifyRights,
        (KW.revoke, KW.view): FX.Privileges.RevokeView,
        (KW.revoke, KW.append): FX.Privileges.RevokeAppend,
        (KW.revoke, KW.host): FX.Privileges.RevokeHost,
        (KW.revoke, KW.discover): FX.Privileges.RevokeDiscover,
        (KW.revoke, KW.modify_rights): FX.Privileges.RevokeModifyRights,
    }
    typ = opts.get((action,privilege), None)
    if typ is None:
        raise Exception(f"Unknown (action,privilege) combination {(action,privilege)}")

    return Effect({
        "type": typ,
        "user": user,
        "target": target
    })

def privileges_type_info(op, curr_type):
    return VT.Effect
from ..core.op_implementations.dispatch_dictionary import _op_to_functions
_op_to_functions[RT.Privileges] = (privileges_implementation, privileges_type_info)

privileges = make_zefop(RT.Privileges)

grant = privileges[KW.grant]
revoke = privileges[KW.revoke]

login = Effect({"type": FX.ZefHub.Login})
logout = Effect({"type": FX.ZefHub.Logout})
# We don't want complaints when these are garbage collected
login._been_run = True
logout._been_run = True
