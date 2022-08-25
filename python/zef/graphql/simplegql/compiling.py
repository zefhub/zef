# Copyright 2022 Synchronous Technologies Pte Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from zef import *
from zef.ops import *
import zef
import zef.pyzef.zefops as pyzefops

import types
import ast
from zef.core.op_implementations.implementation_typing_functions import ZefGenerator

from dataclasses import dataclass
@dataclass
class FunctionDecl:
    inputs: list
    stmts: list
    vararg: str = False
    kwargs: str = False

compilable_ops = {}
compilable_funcs = {}

def maybe_compile_func(obj, *args, allow_const=False, **kwargs):
    if type(obj) in [types.FunctionType, types.BuiltinFunctionType]:
        if obj in compilable_funcs:
            out_func = compile_func(obj.__name__, compilable_funcs[obj], *args, **kwargs, allow_const=allow_const)
        elif len(args) == 0 and len(kwargs) == 0:
            out_func = obj
        else:
            out_func = lambda *more_args,**more_kwargs: obj(*more_args, *args, **more_kwargs, **kwargs)
            out_func._lines = "obj call with args"
            out_func._ann = [("obj", obj)]
            if len(args) > 0:
                out_func._ann += [("args",args)]
            if len(kwargs) > 0:
                out_func._ann += [("kwargs", kwargs)]
    elif type(obj) == ZefOp:
        if len(obj) >= 2:
            assert len(args) == 0 and len(kwargs) == 0, f"Got a zefop chain {obj} with input args {args} or kwargs {kwargs}"
            # Chain zefop logic
            op_chain = list(obj)
            op_chain = [maybe_compile_func(x, allow_const=True) for x in op_chain]
            # As there might be consts in here, we need to bail soon as we come across one
            op_chain = op_chain | reverse | take_until[lambda x: type(x) == ConstResult] | reverse | collect
            # If there is a const, we need to convert this back to a function
            if type(op_chain[0]) == ConstResult:
                if len(op_chain) == 1:
                    out_func = op_chain[0]
                else:
                    out_func = chain_as_func((always[op_chain[0].const], *op_chain[1:]))
            else:
                out_func = chain_as_func(op_chain)
        else:
            # Single zefop logic
            if peel(obj)[0][0] == RT.Function:
                func = peel(obj)[0][1][0][1]
                curried_args = peel(obj)[0][1][1:]
                if len(curried_args) > 0:
                    # Only allow this if we have curried everything in apart
                    # from the initial argument that will be piped.
                    import inspect
                    argspec = inspect.getfullargspec(func)
                    if len(args) + len(curried_args) > 0:
                        if len(argspec.args) != len(curried_args) + len(args) + 1 or argspec.varargs is not None:
                            print(argspec)
                            print(args)
                            print(curried_args)
                            raise Exception("Maybe compiling with curried args won't work because of crazy arg ordering!!!")
                out_func = maybe_compile_func(func, *curried_args, *args, **kwargs)
            else:
                assert len(args) == 0 and len(kwargs) == 0
                op,args = peel(obj)[0]
                if op in compilable_ops:
                    out_func = compile_func(repr(op).replace('.', '__'), compilable_ops[op], *args, allow_const=allow_const)
                else:
                    print(f"WARNING: Op {op} is not compilable!")
                    out_func = obj
    elif type(obj) == CollectingOp:
        collect_func = compile_func("collect", compilable_ops[RT.Collect], allow_const=allow_const)
        if len(obj.el_ops) > 0:
            # First compile the chain as the zefop part
            assert len(args) == 0 and len(kwargs) == 0
            chain_func = maybe_compile_func(ZefOp(obj.el_ops), allow_const=False)

            out_func = lambda x: collect_func(chain_func(x))
        else:
            out_func = collect_func
    elif not hasattr(obj, "__call__"):
        raise Exception(f"Got a non-callable object! {obj}")
    else:
        # raise Exception(f"Don't know how to maybe compile this {type(obj)}")
        # This is duplication
        if len(args) == 0 and len(kwargs) == 0:
            out_func = obj
        else:
            out_func = lambda *more_args,**more_kwargs: obj(*more_args, *args, **more_kwargs, **kwargs)
            out_func._lines = "obj call with args"
            out_func._ann = [("obj", obj)]
            if len(args) > 0:
                out_func._ann += [("args",args)]
            if len(kwargs) > 0:
                out_func._ann += [("kwargs", kwargs)]

    if out_func is None:
        return obj

    return out_func


def chain_as_func(op_chain):
    # This op chain might be made up of ZefOps + funcs. The only tricky part in
    # here is that we want to cause ZefOps to return ZefGenerators rather than
    # fully evaluate their output.
    inputs = ["x"]
    stmts = []
    for op in op_chain:
        stmts.append(PartialStatement("x",
                                      op,
                                      "x"))
    stmts.append(ReturnStatement("x"))

    func_decl = FunctionDecl(inputs=inputs, stmts=stmts)
        
    return stmts_to_pyfunc("chain", func_decl)

def zefop_call(op, *args):
    if len(args) == 1:
        lv = LazyValue(args[0]) | op
    else:
        from zef.ops import unpack
        lv = LazyValue(args) | unpack[op]
    return lv.evaluate(unpack_generator=False)
    


func_cache = {}

def compile_func(name, stmts_func, *args, allow_const=False, **kwargs):
    from .hashables import freeze
    key = (name, *freeze(args), "*", *freeze(sorted(kwargs.items())))
    pyfunc = func_cache.get(key, None)
    if pyfunc is None:
        # print(f"Going to compile pyfunc for {key}")
        stmts_ret = stmts_func(*args, **kwargs)
        if stmts_ret is None:
            pyfunc = None
        elif type(stmts_ret) == ConstResult:
            pyfunc = stmts_ret
        elif type(stmts_ret) in [types.FunctionType, ZefOp, types.BuiltinFunctionType]:
            pyfunc = maybe_compile_func(stmts_ret, allow_const=True)
        elif type(stmts_ret) == FunctionDecl:
            pyfunc = stmts_to_pyfunc(name, stmts_ret)
        else:
            raise Exception(f"stmts_func ({stmts_func}) returned something unexpected: {type(stmts_ret)}")
        func_cache[key] = pyfunc

    if not allow_const:
        if type(pyfunc) == ConstResult:
            # The varargs here is bad. TODO fix
            return lambda *args, val=pyfunc.const: val

    return pyfunc

    
class PartialStatement:
    def __init__(self, inputs, op, outputs, *, starargs=False):
        self.inputs = inputs
        self.op = op
        self.outputs = outputs
        self.starargs = starargs
    def __str__(self):
        return f"PartialStatement(in={self.inputs}, op={compiled_func_summary(self.op)}, out={self.outputs}, starargs={self.starargs})"

class AssignStatement:
    def __init__(self, const, outputs):
        self.const = const
        self.outputs = outputs
    def __str__(self):
        return f"AssignStatement(const={self.const}, out={self.outputs})"

class ReturnStatement:
    def __init__(self, names):
        self.names = names
    def __str__(self):
        return f"ReturnStatement(name={self.names}))"

class RawASTStatement:
    def __init__(self, ast):
        self.ast = ast
    def __str__(self):
        return f"RawASTStatement(ast={self.ast})"

class ConstResult:
    def __init__(self, const):
        self.const = const

def stmts_to_pyfunc(name, func_decl):
    last_i = 0
    def name_gen(prefix="anon"):
        nonlocal last_i
        last_i += 1

        return f"{prefix}__{last_i}"
    
    boring = {"lineno": 0, "col_offset": 0}

    body = []
    globs = {}
    # cur_var_list = inputs

    def var_or_tuple(names, ctx):
        if type(names) == str:
            return ast.Name(id=names, ctx=ctx, **boring)
        else:
            return ast.Tuple(elts=[ast.Name(id=x, ctx=ctx, **boring) for x in names], ctx=ctx, **boring)
        
    for stmt in func_decl.stmts:
        if type(stmt) == RawASTStatement:
            if type(stmt.ast) == str:
                parsed_mod = ast.parse(stmt.ast)
                body += parsed_mod.body
            elif type(stmt.ast) == ast.Module:
                body += stmt.ast.body
            else:
                raise Exception(f"Don't know how to handle raw ast of type {type(stmt.ast)}")
        elif type(stmt) == AssignStatement:
            # Repetition!!!
            output_var = var_or_tuple(stmt.outputs, ast.Store())

            if type(stmt.const) in [str, int, float, types.NoneType]:
                value = ast.Constant(value=stmt.const)
            else:
                const_name = name_gen("const")
                globs[const_name] = stmt.const
                value = ast.Name(id=const_name, ctx=ast.Load(), **boring)

            const_stmt = ast.Assign(
                targets=[output_var],
                value=value,
                **boring,
            )

            body += [const_stmt]
        elif type(stmt) == PartialStatement:
            # See if we can compile the op itself
            compiled_op = maybe_compile_func(stmt.op, allow_const=True)

            if type(compiled_op) == ConstResult:
                output_var = var_or_tuple(stmt.outputs, ast.Store())

                if type(compiled_op.const) in [str, int, float, types.NoneType]:
                    value = ast.Constant(value=compiled_op.const)
                else:
                    const_name = name_gen("const")
                    globs[const_name] = compiled_op.const
                    value = ast.Name(id=const_name, ctx=ast.Load(), **boring)
                    
                const_stmt = ast.Assign(
                    targets=[output_var],
                    value=value,
                    **boring,
                )

                body += [const_stmt]
            else:
                # input_name = name_gen("input")
                # value = var_or_tuple(stmt.inputs, ast.Load())
                # input_stmt = ast.Assign(
                #     targets=[ast.Name(id=input_name, ctx=ast.Store(), **boring)],
                #     value=value,
                #     **boring
                # )
                input_item = var_or_tuple(stmt.inputs, ast.Load())

                output_var = var_or_tuple(stmt.outputs, ast.Store())

                func_name = name_gen("func")
                if type(compiled_op) == ZefOp:
                    globs[func_name] = lambda *args, op=compiled_op: zefop_call(op, *args)
                    globs[func_name]._lines = "zefop_call(...)"
                    globs[func_name]._ann = [("op", compiled_op)]
                else:
                    globs[func_name] = compiled_op

                # call_args = ast.Name(id=input_name, ctx=ast.Load(), **boring)
                call_args = input_item
                if stmt.starargs:
                    call_args = ast.Starred(call_args)
                call_expr = ast.Call(
                    func=ast.Name(id=func_name, ctx=ast.Load(), **boring),
                    args=[call_args],
                    keywords=[]
                )
                assign_stmt = ast.Assign(
                    targets=[output_var],
                    value=call_expr,
                    **boring,
                )

                # body += [input_stmt, assign_stmt]
                body += [assign_stmt]
        elif type(stmt) == ReturnStatement:
            ret_var = var_or_tuple(stmt.names, ast.Load())
            ret_stmt = ast.Return(value=ret_var, **boring)
            body += [ret_stmt]
        else:
            raise Exception(f"Don't understand statement {type(stmt)}")


    body += [ast.Raise(exc=ast.parse('Exception("Should never get to end of function")', mode="single").body[0].value,
                      **boring
                      )]
    args = ast.arguments(posonlyargs=[],
                         args=[ast.arg(x, **boring) for x in func_decl.inputs],
                         kwonlyargs=[],
                         kw_defaults=[],
                         defaults=[],
                         vararg=(None if func_decl.vararg is False else ast.arg(func_decl.vararg, **boring)),
                         kwarg=(None if func_decl.kwargs is False else ast.arg(func_decl.kwargs, **boring)),
                         )
    f = ast.FunctionDef(name=name, args=args, body=body, decorator_list=[], **boring)

    full_ast = ast.Interactive([f])

    # This is dodgy, but is an attempt to preserve code along with the function
    lines = ast.unparse(full_ast)
    co = compile(lines, f"{name}-compiled", "single")

    exec(co, globs)
    func = globs[name]
    func._lines = lines
    return func



def add_indent(s):
    return s | split['\n'] | map[lambda x: "|  " + x] | collect

def compiled_func_as_str(f):
    s = []
    if hasattr(f, "_lines"):
        s += [f"{f} with code:\n{f._lines}"]
        if hasattr(f, "__code__"):
            globs = f.__code__.co_names | filter[contains["__"]] | collect
            for glob in globs:
                s += [f"With glob {glob}:"]
                nested = compiled_func_as_str(f.__globals__[glob])
                s += add_indent(nested)
        else:
            print("Can't get globs")
                
    else:
        s += [f"obj: {f!r}"]
    if hasattr(f, "_ann"):
        for name,val in f._ann:
            val_s = compiled_func_as_str(val)
            if '\n' in val_s:
                s += [f"Annotation, {name}::"] + add_indent(val_s)
            else:
                s += [f"Annotation, {name} = {val_s}"]
    return '\n'.join(s)

def compiled_func_summary(f):
    if type(f) == types.FunctionType:
        return f"Func: {f.__name__}"
    return str(f)

    
        

##############################
# * Test case
#----------------------------

def stmts_simple_func(copy_rt, is_out=True, pred=None):
    inputs = ["z"]
    stmts = []
    
    rt = RT(copy_rt)
    
    if is_out is True:
        stmts.append(PartialStatement("z",
                                      Outs[rt],
                                      "out"))
    elif is_out is False:
        stmts.append(PartialStatement("z",
                                      Ins[rt],
                                      "out"))
    else:
        assert is_out == "both"
        stmts.append(PartialStatement("z",
                                      Outs[rt],
                                      "a"))
        stmts.append(PartialStatement("z",
                                      Ins[rt],
                                      "b"))
        if pred is None:
            pred = length | greater_than[0]
        stmts.append(PartialStatement(["a","b"],
                                      (And
                                       [first | pred]
                                       [second | pred]
                                       ),
                                      "out"))
    stmts.append(ReturnStatement("out"))
    
    return FunctionDecl(inputs=inputs, stmts=stmts)
    


##############################
# * Ops
#----------------------------


def stmts_All(et):
    if type(et) != EntityType:
        raise Exception("Fast all is only for ETs.")

    return lambda gs: gs.tx | pyzefops.instances[et]

compilable_ops[RT.All] = stmts_All

def stmts_And(*preds):
    inputs = ["z"]
    stmts = []

    if len(preds) == 0:
        return ConstResult(True)

    if len(preds) == 1:
        return preds[0]

    # # This will just fallback to a call to And_implementation.
    # return None

    compiled_preds = (preds
                      | map[lambda x: maybe_compile_func(x, allow_const=True)]
                      | filter[Not[lambda x: type(x) == ConstResult and x.const is True]]
                      | collect)
    if len(compiled_preds) == 0:
        return ConstResult(True)
    if compiled_preds | map[lambda x: type(x) == ConstResult and x.const is False] | any | collect:
        return ConstResult(False)

    all_names = []
    for i,pred in enumerate(compiled_preds):
        name = f"pred_{i}"
        stmts += [AssignStatement(pred, name)]
        all_names += [name]
    stmts += [RawASTStatement("preds = [" + ', '.join(all_names) + "]")]
    from zef.core.op_implementations.implementation_typing_functions import and_imp
    stmts += [
        PartialStatement(["z", *all_names],
                         and_imp,
                         "out",
                         starargs=True),
        ReturnStatement("out")
    ]

    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.And] = stmts_And

def stmts_Or(*preds):
    inputs = ["z"]
    stmts = []

    if len(preds) == 0:
        return ConstResult(False)

    if len(preds) == 1:
        return preds[0]

    # # This will just fallback to a call to And_implementation.
    # return None

    compiled_preds = (preds
                      | map[lambda x: maybe_compile_func(x, allow_const=True)]
                      | filter[Not[lambda x: type(x) == ConstResult and x.const is False]]
                      | collect)
    if len(compiled_preds) == 0:
        return ConstResult(False)
    if compiled_preds | map[lambda x: type(x) == ConstResult and x.const is True] | any | collect:
        return ConstResult(True)

    all_names = []
    for i,pred in enumerate(compiled_preds):
        name = f"pred_{i}"
        stmts += [AssignStatement(pred, name)]
        all_names += [name]
    stmts += [RawASTStatement("preds = [" + ', '.join(all_names) + "]")]
    from zef.core.op_implementations.implementation_typing_functions import or_imp
    stmts += [
        PartialStatement(["z", *all_names],
                         or_imp,
                         "out",
                         starargs=True),
        ReturnStatement("out")
    ]

    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.Or] = stmts_Or

def stmts_always(val):
    return ConstResult(val)

compilable_ops[RT.Always] = stmts_always

def stmts_not(func):
    cfunc = maybe_compile_func(func, allow_const=True)
    if type(cfunc) == ConstResult:
        return ConstResult(not(cfunc))

    stmts = [
        PartialStatement("x",
                         func,
                         "result"),
        RawASTStatement("return not result"),
    ]
    return FunctionDecl(inputs=["x"], stmts=stmts)

compilable_ops[RT.Not] = stmts_not

def stmts_equals(val):
    stmts = [
        AssignStatement(val,
                        "val"),
        RawASTStatement("return x == val"),
    ]
    return FunctionDecl(inputs=["x"], stmts=stmts)

compilable_ops[RT.Equals] = stmts_equals

def stmts_contained_in(l):
    stmts = [
        AssignStatement(l,
                        "l"),
        RawASTStatement("return x in l"),
    ]
    return FunctionDecl(inputs=["x"], stmts=stmts)

compilable_ops[RT.ContainedIn] = stmts_contained_in

def stmts_identity():
    return FunctionDecl(inputs=["x"],  stmts=[ReturnStatement("x")])

compilable_ops[RT.Identity] = stmts_identity

def stmts_filter(pred):
    inputs = ["input"]
    stmts = []

    cpred = maybe_compile_func(pred, allow_const=True)
    if type(cpred) == ConstResult:
        if cpred.const is True:
            return identity
        elif cpred.const is False:
            return ConstResult([])
        else:
            raise Exception("Filter doesn't return Bool")

    def filter_fast(v, f):
        for el in v:
            if f(el):
                yield el

    from zef.core.op_implementations.implementation_typing_functions import filter_implementation
    stmts += [
        AssignStatement(cpred, "pred"),
        # AssignStatement(filter_implementation, "filter_implementation"),
        # RawASTStatement("out = filter_implementation(input, pred)"),
        PartialStatement(["input", "pred"], filter_fast, "out", starargs=True),
        ReturnStatement("out")
    ]

    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.Filter] = stmts_filter

def stmts_map(func):
    inputs = ["input"]
    stmts = []

    cfunc = maybe_compile_func(func)

    # from zef.core.op_implementations.implementation_typing_functions import map_implementation
    def map_fast(v, f):
        for el in v:
            yield(f(el))
    stmts += [
        AssignStatement(cfunc, "func"),
        PartialStatement(["input", "func"], map_fast, "out", starargs=True),
        ReturnStatement("out")
    ]

    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.Map] = stmts_map

def stmts_greater_than(limit):
    inputs = ["x"]
    stmts = [AssignStatement(limit, "limit")]
    stmts += [RawASTStatement("return x > limit")]
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.GreaterThan] = stmts_greater_than

def stmts_take(n):
    def take_fast(v):
        i = 0
        for el in v:
            yield(el)
            i += 1
            if i >= n:
                break
        
    return take_fast

compilable_ops[RT.Take] = stmts_take

def stmts_length():
    inputs = ["x"]
    stmts = [PartialStatement("x", len, "out")]
    stmts += [ReturnStatement("out")]
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.Length] = stmts_length

def stmts_single():
    inputs = ["x"]
    stmts = [RawASTStatement("assert len(x) == 1")]
    stmts += [RawASTStatement("return x[0]")]
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.Single] = stmts_single

def stmts_first():
    inputs = ["x"]
    stmts = [RawASTStatement("return x[0]")]
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.First] = stmts_first

def stmts_second():
    inputs = ["x"]
    stmts = [RawASTStatement("return x[1]")]
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.Second] = stmts_second

def stmts_to_ezefref():
    return lambda x: pyzefops.to_ezefref(x)

compilable_ops[RT.ToEZefRef] = stmts_to_ezefref

def stmts_get_field(field):
    inputs = ["x"]
    stmts = [RawASTStatement(f"return x.{field}")]
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.GetField] = stmts_get_field

def stmts_get(name, *args):
    inputs = ["x"]
    stmts = []
    if len(args) == 0:
        stmts += [RawASTStatement(f"return x['{name}']")]
    elif len(args) == 1:
        stmts += [
            AssignStatement(args[0], "default"),
            RawASTStatement(f"return x.get('{name}', default)")
        ]
    else:
        raise Exception("get should take 1 or 2 arguments")
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.Get] = stmts_get

def stmts_Outs(rt=None, target_filter=None):
    return out_rels[rt][target_filter] | map[target]

compilable_ops[RT.Outs] = stmts_Outs

def stmts_out_rels(rt=None, target_filter=None):
    # Being lazy
    if type(rt) != RelationType:
        return None
    if target_filter is not None:
        return None

    return maybe_compile_func(pyzefops.traverse_out_edge_multi, rt)

compilable_ops[RT.OutRels] = stmts_out_rels

def stmts_Out(rt=None, target_filter=None):
    return Outs[rt][target_filter] | single

compilable_ops[RT.Out] = stmts_Out


def stmts_in_rels(rt=None, source_filter=None):
    # Being lazy
    if type(rt) != RelationType:
        return None
    if source_filter is not None:
        return None

    return maybe_compile_func(pyzefops.traverse_in_edge_multi, rt)

compilable_ops[RT.InRels] = stmts_in_rels

def stmts_in_rel(rt=None, source_filter=None):
    return in_rels[rt][source_filter] | single

compilable_ops[RT.InRel] = stmts_in_rel

def stmts_single_or(default):
    # inputs = ["l"]
    # stmts = [
    #     AssignStatement(default, "default"),
    #     RawASTStatement("res = default if len(l) == 1 else l[0]"),
    #     ReturnStatement("res")
    # ]
    # return inputs,stmts
    from zef.core.op_implementations.implementation_typing_functions import single_or_imp
    return lambda l: single_or_imp(l, default)

compilable_ops[RT.SingleOr] = stmts_single_or

def stmts_value_or(default):
    inputs = ["z"]
    stmts = [
        AssignStatement(default, "default"),
        AssignStatement(value, "value"),
        RawASTStatement("res = default if z is None else value(z)"),
        ReturnStatement("res")
    ]
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.ValueOr] = stmts_value_or

def stmts_target():
    return pyzefops.target

compilable_ops[RT.Target] = stmts_target

def stmts_match(opts):
    inputs = ["z"]
    stmts = []

    for case,action in opts:
        c_action = maybe_compile_func(action)
        stmts += [
            PartialStatement("z", is_a[case], "res"),
            AssignStatement(c_action, "action"),
            RawASTStatement("if res: return action(z)")
        ]
        
    stmts += [RawASTStatement("raise Exception('no match')")]
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.Match] = stmts_match

def stmts_is_a(typ):
    inputs = ["z"]
    stmts = []

    if typ == Any:
        return ConstResult(True)
    elif typ in [ZefRef, EZefRef, Graph]:
        stmts += [AssignStatement(typ, "typ"),
                  RawASTStatement("return type(z) == typ")]
    elif type(typ) in [EntityType, RelationType, AttributeEntityType]:
        stmts += [AssignStatement(typ, "typ"),
                  PartialStatement("z", rae_type, "z_typ"),
                  RawASTStatement("return z_typ == typ")]
    elif typ == ET:
        stmts += [AssignStatement(BT.ENTITY_NODE, "target_bt"),
                  PartialStatement("z", BT, "bt"),
                  RawASTStatement("return bt == target_bt")]
    elif typ == RT:
        stmts += [AssignStatement(BT.RELATION_EDGE, "target_bt"),
                  PartialStatement("z", BT, "bt"),
                  RawASTStatement("return bt == target_bt")]
    elif typ == AET:
        stmts += [AssignStatement(BT.ATTRIBUTE_ENTITY_NODE, "target_bt"),
                  PartialStatement("z", BT, "bt"),
                  RawASTStatement("return bt == target_bt")]
    elif type(typ) == ValueType_ and typ.d["type_name"] == "Is":
        stmts += [PartialStatement("z",
                                   maybe_compile_func(typ.d["absorbed"][0]),
                                   "res"),
                  ReturnStatement("res")]
    else:
        raise Exception(f"Don't know how to compile is_a for {typ!r}")
    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.IsA] = stmts_is_a

def stmts_rae_type():
    # The lambdas are here so that we don't query things on types but only call them
    return match[(RT, lambda x: RT(x)),
                 (ET, lambda x: ET(x)),
                 (AET, lambda x: AET(x))]

compilable_ops[RT.RaeType] = stmts_rae_type

class Missing:
    pass

def stmts_get_in(path, *args):
    inputs = ["d"]
    stmts = []

    if len(args) == 0:
        has_default = False
    elif len(args) == 1:
        has_default = True
        stmts += [AssignStatement(args[0], "default")]
    else:
        raise Exception("Don't know what extra args mean.")

    stmts += [AssignStatement(Missing, "Missing")]
    for item in path:
        stmts += [PartialStatement("d", get[path][Missing], "d")]
        if has_default:
            stmts += [RawASTStatement("if d is Missing: return default")]
        else:
            stmts += [RawASTStatement("if d is Missing: raise KeyError")]
    stmts += [ReturnStatement("d")]

compilable_ops[RT.GetIn] = stmts_get_in


def stmts_value():
    return pyzefops.value

compilable_ops[RT.Value] = stmts_value

def stmts_collect():
    def collect_fast(x):
        if hasattr(x, "__iter__"):
            return list(x)
        else:
            return x
    return collect_fast

compilable_ops[RT.Collect] = stmts_collect

def stmts_origin_uid():
    inputs = ["z"]
    stmts = []

    stmts += [AssignStatement(ZefRef, "ZefRef")]
    stmts += [RawASTStatement("assert type(z) == ZefRef, f'type is not ZefRef: {type(z)}: {z}'")]
    stmts += [PartialStatement("z",
                               to_ezefref | in_rel[BT.RAE_INSTANCE_EDGE] | Outs[BT.ORIGIN_RAE_EDGE] | collect,
                               "origin_candidates")]
                               
    stmts += [PartialStatement(["z","origin_candidates"],
                               match[(Is[second | length | greater_than[0]],
                                      second | only | Out[BT.ORIGIN_GRAPH_EDGE] | base_uid),
                                     (Any, first | to_ezefref | uid)],
                               "out")]
    stmts += [ReturnStatement("out")]

    return FunctionDecl(inputs=inputs, stmts=stmts)

compilable_ops[RT.OriginUid] = stmts_origin_uid


def stmts_uid():
    return lambda x: pyzefops.uid(x)

compilable_ops[RT.Uid] = stmts_uid

def stmts_base_uid():
    return lambda x: pyzefops.uid(x).blob_uid

compilable_ops[RT.BaseUid] = stmts_base_uid