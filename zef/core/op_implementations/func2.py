from .._ops import *
from .._core import *

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