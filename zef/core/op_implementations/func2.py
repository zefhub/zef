from .._ops import *
from .._core import *
from ... import core

class FunctionZefOp:
    @staticmethod
    def __call__(*args, **kwds):
        from types import FunctionType
        if len(kwds) == 0 and len(args) == 1 and isinstance(args[0], FunctionType):
            return ZefOp(((RT.Function, ((2, args[0]), )), ))
        else:
            from zef.core.zef_functions import zef_function_decorator
            promote_to_zefref_func = zef_function_decorator(*args, **kwds)
            def inner(func):
                zefref = promote_to_zefref_func(func)
                # abstract_entity = Entity(zefref)
                # TODO return here the abstract Entity! instead of ZefRef
                return ZefOp(((RT.Function, ((0, zefref), )), ))
            return inner

    @staticmethod
    def __getitem__(arg):
        # TODO we gotta check if arg is of type Zef Lambda once we implement it
        # return ZefOp(((RT.Function, ((1, arg), )), ))
        return ZefOp(((RT.Function, ((2, arg), )), ))

func2 = FunctionZefOp()

def function_imp(x0, func_info, *args, **kwargs):
    """
    func_repr is of form (int, any)
    where the first integer encodes how the function is
    represented in the ZefOp:
    -------- representation types -------
    0) Abstract Entity
    1) Zef Lambda
    2) captured python lambda or local function
    """
    repr_indx, fct = func_info
    if repr_indx == 0:
        # TODO: Convert Abstract Entity to ZefRef? Or implement Abstract Entity specfic caching and compilation!

        # extract the atomic entity and look in the compiled function cache.
        # apply that function to the args, similar to the case below
        from zef.core.zef_functions import _overloaded_zefref_call
        return _overloaded_zefref_call(fct, x0, *args, **kwargs)
    if repr_indx == 2:
        return fct(x0, *args, **kwargs)
    else:
        raise NotImplementedError('Zef Lambda expressions is not implemented yet.')


core.op_implementations.dispatch_dictionary._op_to_functions[RT.Function] = (function_imp, None)