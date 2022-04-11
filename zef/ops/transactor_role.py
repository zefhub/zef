__all__ = [
    "take_transactor_role",
    "release_transactor_role",
    "have_transactor_role",
]

from ..core import *
from ..core._ops import *

from ..core.op_implementations.dispatch_dictionary import _op_to_functions

def take_transactor_role_implementation(g):
    return Effect({"type": FX.Graph.TakeTransactorRole,
                   "graph": g})
def take_transactor_role_tp(x):
    return VT.Any
def release_transactor_role_implementation(g):
    return Effect({"type": FX.Graph.ReleaseTransactorRole,
                   "graph": g})
def release_transactor_role_tp(x):
    return VT.Any

def have_transactor_role_implementation(g):
    return g.graph_data.is_primary_instance
def have_transactor_role_tp(x):
    return VT.Bool

_op_to_functions[RT.TakeTransactorRole] = (take_transactor_role_implementation, take_transactor_role_tp)
_op_to_functions[RT.ReleaseTransactorRole] = (release_transactor_role_implementation, release_transactor_role_tp)
_op_to_functions[RT.HaveTransactorRole] = (have_transactor_role_implementation, have_transactor_role_tp)

take_transactor_role = make_zefop(RT.TakeTransactorRole)
release_transactor_role = make_zefop(RT.ReleaseTransactorRole)
have_transactor_role = make_zefop(RT.HaveTransactorRole)