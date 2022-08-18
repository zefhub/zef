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

import types

compilable_ops = {}
compilable_funcs = {}

def maybe_compile_func(obj, *args, allow_const=False, **kwargs):
    if type(obj) == types.FunctionType:
        if obj in compilable_funcs:
            out_func = compile_func(obj.__name__, compilable_funcs[obj], *args, **kwargs, allow_const=allow_const)
        elif len(args) == 0 and len(kwargs) == 0:
            out_func = obj
        else:
            out_func = lambda *more_args,**more_kwargs: obj(*more_args, *args, **more_kwargs, **kwargs)
    elif type(obj) == ZefOp:
        assert len(args) == 0 and len(kwargs) == 0
        if len(obj) >= 2:
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
                args = peel(obj)[0][1][1:]
                print(peel(obj))
                print(func)
                print(args)
                out_func = maybe_compile_func(func, *args)
            else:
                op,args = peel(obj)[0]
                if op in compilable_ops:
                    out_func = compile_func(repr(op).replace('.', '__'), compilable_ops[op], *args, allow_const=allow_const)
                else:
                    out_func = obj
    else:
        raise Exception(f"Don't know how to maybe compile this {type(obj)}")

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
        if type(op) == ZefOp:
            # Magic with ZefGenerator
            pass
        else:
            stmts.append(PartialStatement("x",
                                          op,
                                          "x"))
    stmts.append(ReturnStatement("x"))
        
    return stmts_to_pyfunc("chain", inputs, stmts)

def zefop_call(op, starargs, *args):
    if starargs:
        from zef.ops import unpack
        lv = args | unpack[op]
    else:
        lv = args | op
    return lv.evalute(unpack_generators=False)
    


func_cache = {}

def compile_func(name, stmts_func, *args, allow_const=False, **kwargs):
    from .hashables import freeze
    key = (name, *freeze(args), "*", *freeze(sorted(kwargs.items())))
    pyfunc = func_cache.get(key, None)
    if pyfunc is None:
        print(f"Going to compile pyfunc for {name} with args={args}")
        stmts_ret = stmts_func(*args, **kwargs)
        if stmts_ret is None:
            pyfunc = None
        elif type(stmts_ret) == ConstResult:
            pyfunc = stmts_ret
        elif type(stmts_ret) == types.FunctionType:
            pyfunc = stmts_ret
        else:
            inputs,stmts = stmts_ret
            pyfunc = stmts_to_pyfunc(name, inputs, stmts)
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

class AssignStatement:
    def __init__(self, const, outputs):
        self.const = const
        self.outputs = outputs

class ReturnStatement:
    def __init__(self, names):
        self.names = names

class ConstResult:
    def __init__(self, const):
        self.const = const

def stmts_to_pyfunc(name, inputs, stmts):
    last_i = 0
    def name_gen(prefix="anon"):
        nonlocal last_i
        last_i += 1

        return f"{prefix}__{last_i}"
    
    import ast

    boring = {"lineno": 0, "col_offset": 0}

    body = []
    globs = {}
    # cur_var_list = inputs

    def var_or_tuple(names, ctx):
        if type(names) == str:
            return ast.Name(id=names, ctx=ctx, **boring)
        else:
            return ast.Tuple(elts=[ast.Name(id=x, ctx=ctx, **boring) for x in names], ctx=ctx, **boring)
        
    for stmt in stmts:
        if type(stmt) == AssignStatement:
            # Repetition!!!
            output_var = var_or_tuple(stmt.outputs, ast.Store())

            if type(stmt.const) in [str, int, float, types.NoneType]:
                value = ast.Const(value=stmt.const)
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
                    value = ast.Const(value=compiled_op.const)
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
                input_name = name_gen("input")
                value = var_or_tuple(stmt.inputs, ast.Load())
                input_stmt = ast.Assign(
                    targets=[ast.Name(id=input_name, ctx=ast.Store(), **boring)],
                    value=value,
                    **boring
                )

                output_var = var_or_tuple(stmt.outputs, ast.Store())

                func_name = name_gen("func")
                if type(compiled_op) == ZefOp:
                    globs[func_name] = lambda *args, op=compiled_op: zefop_call(compiled_op, stmt.starargs, *args)
                else:
                    globs[func_name] = compiled_op

                call_args = ast.Name(id=input_name, ctx=ast.Load(), **boring)
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

                body += [input_stmt, assign_stmt]
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
                         args=[ast.arg(x, **boring) for x in inputs],
                         kwonlyargs=[],
                         kw_defaults=[],
                         defaults=[],
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
    
    return inputs,stmts
    


def stmts_And(*preds):
    inputs = ["z"]
    stmts = []

    if len(preds) == 0:
        return ConstResult(True)

    if len(preds) == 1:
        return maybe_compile_func(preds[0], allow_const=True)

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

    # from zef.core.op_implementations.implementation_typing_functions import and_imp
    # stmts += [PartialStatement("z",
    #                            lambda x: and_imp(x, *compiled_preds),
    #                            "out"),
    #           ReturnStatement("out")]

    from zef.core.op_implementations.implementation_typing_functions import and_imp
    stmts += [AssignStatement(compiled_preds,
                              "preds"),
              PartialStatement(["z", "preds"],
                               lambda z, preds: and_imp(z, *preds),
                               "out",
                               starargs=True),
              ReturnStatement("out")]

    return inputs,stmts

compilable_ops[RT.And] = stmts_And

def stmts_always(val):
    return ConstResult(val)

compilable_ops[RT.Always] = stmts_always