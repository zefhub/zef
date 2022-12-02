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


from .. import report_import
report_import("zef.core.error")

custom_error_handling = True
import os
if os.environ.get("ZEF_ERROR_HANDLING", "").lower() in ["0", "false", "no", "off"]:
    custom_error_handling = False
def custom_error_handling_activated():
    return custom_error_handling

# This is an exception only for our internal evaluation engine logic. This error
# should never propagate outside of the evaluation engine, and instead be
# converted to an ExceptionWrapper, which will print nicely.
class Error_(Exception):
    def __init__(self, name):
        self.name = name

    # def __set_name__(self, parent, name):
    #     self.name = name

    def __call__(self, *args):
        err = Error_(self.name)
        err.args = args
        err.contexts = []
        err.nested = None
        return err

    def __repr__(self):
        if not self.args or len(self.args) == 0: args = "()"
        elif len(self.args) == 1: args = f"({repr(self.args[0])})"
        else: args = self.args
        return f'{self.name}{args}'

    def __str__(self):
        try:
            # We add an extra newline at the start to better handle the output
            return "\n" + zef_ui_err(self, as_str=True)
        except Exception as e:
            try:
                e_s = str(e)
            except:
                e_s = "Can't take str of failure exception"
            return f"Failed in displaying zef error: {e_s}"
        

    def __eq__(self, other):
        if not isinstance(other, Error_): return False
        return self.name == other.name and self.args == other.args

    def __bool__(self):
        return False

def zef_ui_err_fallback(self):
    try:
        return "\n\nThis is a custom error output for a wrapped Zef error\n\n" + str_zef_error(self)
    except Exception as exc:
        import traceback
        traceback.print_tb(exc.__traceback__)


def zef_ui_err(err, as_str=False):
    from ..ops import contains,last, get, collect, filter, ZefOp, LazyValue
    from ..ui import Text,VStack, Frame, show, Code, to_rich_str
    from ..core.op_implementations.implementation_typing_functions import ZefGenerator

    def truncate_obj_str(obj, max_len=20):
        obj_str = str(obj)
        index_of_newline = obj_str.find('\n')
        if index_of_newline != -1: obj_str = obj_str[:index_of_newline] + "..."
        if len(obj_str) > max_len: obj_str = obj_str[:max_len] + "... "
        return obj_str

    def make_lazy_value_pretty(lzy_val: LazyValue) -> str:
        if not isinstance(lzy_val, LazyValue): return str(lzy_val)
        initial_value = truncate_obj_str(lzy_val.initial_val)
        internal_ops = lzy_val.el_ops.el_ops
        internal_ops = ZefOp(internal_ops,)
        return f"{initial_value} | {internal_ops}" 
    
    MAX_CONSOLE_WIDTH = 115
    try:
        # Required Information
        nested = err.nested
        name = err.name
        contexts = err.contexts
        frames = contexts | filter[contains["frames"]] | collect 
        if frames:
            top_frame = frames | last | get["frames"] | last | collect 
        else:
            top_frame = None
        type_check = contexts | filter[contains["type_check"]] | collect

        chains = contexts | filter[contains["chain"]] | collect

        states = contexts | filter[contains["state"]] | collect

        metadata = contexts | filter[contains["metadata"]] | collect
        if len(metadata) == 1: 
            metadata = metadata[0]['metadata']
        else:
            metadata = None


        #### Title ####
        title = Text(name, color="#FF9494", italic=True)


        stack_lst = []
        ##### Header ####
        err_type = nested.get('type', None)
        if top_frame and err_type:
            stack_lst += [Text(f"\n{nested['type']} occured in {top_frame['func_name']}", color="#189ad3")]
        elif err_type:
            stack_lst += [Text(f"\n{nested['type']}", color="#189ad3")]


        ##### Frames ####
        if top_frame:
            fname = top_frame['filename']
            try:
                truncated_filename = fname[fname.rindex("zef"):]
            except:
                truncated_filename = fname
            line_no = f":{top_frame['lineno']}" if top_frame['lineno'] else ""
            file_line = Text(f"~/{truncated_filename}{line_no}", color="#33b864")
            file_text = Text(["File ", file_line])
            stack_lst += [file_text]

        ##### Carret Highlighting ####
        if chains:
            chain_str   = make_lazy_value_pretty(chains[0]['chain'])
            arrow_str   = "--> "
            if len(chain_str) + len(arrow_str) > MAX_CONSOLE_WIDTH:
                chain_str = chain_str[:MAX_CONSOLE_WIDTH - len(arrow_str) - 5] + "... "  
            stack_lst += [Text([Text("\n--> ", color="#33b864"), chain_str])]

            def make_carrets_if_found(chain_str, failed_func, arrow_str):
                if "_implementation" in failed_func: failed_func = failed_func[:failed_func.index("_implementation")]
                if "_imp" in failed_func: failed_func = failed_func[:failed_func.index("_imp")]
                idx = chain_str.find(failed_func) 
                if idx == -1: 
                    return []
                padding = idx + len(arrow_str)
                error_carrets = f"{' ' * padding}{'^' * len(failed_func)}"
                return  [Text(error_carrets, color="#FF9494")]

            failed_func = str(ZefOp((chains[-1]['op'],),))
            stack_lst += make_carrets_if_found(chain_str, failed_func, arrow_str)

        ##### States or Context #####
        if len(states) < 1:
            chain = chains[0]
            context_header = Text("\n==Context==\n", bold=True, justify="center", italic=True)
            code_str = f"""
    chain = {make_lazy_value_pretty(chain['chain'])}
    input = {truncate_obj_str(chain['input'], 40)}
    op    = {ZefOp((chain['op'],),)}
            """
            context_code = Code(code_str, language = "python3")
            stack_lst += [context_header, context_code]
        else:
            state_header = Text("\n==States==\n", bold=True, justify="center", italic=True)
            state_stack = []

            for i, chain in enumerate(reversed(chains)):
                if isinstance(chain['input'], ZefGenerator): break
                code_str = f"""
    input = {truncate_obj_str(chain['input'], 40)}
            """
                chain_state = Code(code_str, language = "python3")
                state_frame = Frame(chain_state, title=repr(ZefOp((chain['op'],),)))
                state_stack.append(state_frame)

            stack_lst += [state_header, *state_stack]


        ##### Type Checking ####
        tc_header = Text("\n==Type Checking==\n", bold=True, justify="center", italic=True)
        tc_fail_body = Text("Failed to do Type Checking due to missing annotations", color="#FF9494", bold=True)

        if type_check:
            tc_stack = []
            for t_check in type_check:
                t_check = t_check['type_check']
                if t_check:
                    tc_fn = f"{t_check['function'].__name__}"
                    tc_body = f"Expected: {t_check['expected']['arg']} = {t_check['expected']['type']}\n"
                    tc_check = f"Type Check against `{t_check['expected']['input']}`: "
                    tc_result =  ['Failed ❌','Success ✅'][t_check['result']]
                    tc_body = Frame(Text([tc_body,tc_check, tc_result], color="#FF9494"), title=Text(str(tc_fn)))
                    tc_stack.append(tc_body)
            
            if tc_stack: stack_lst += [tc_header, *tc_stack]
            else: stack_lst += [tc_header, tc_fail_body]
        else:
            stack_lst += [tc_header, tc_fail_body]

        ##### Error Messages ####
        if nested.get('args', None):
            err_msg_header = Text("\n==Error Message==", bold=True, justify="center", italic=True)
            err_msg = Text(f"\n{','.join([str(arg) for arg in nested['args']])}", color="#FF9494")
            stack_lst += [err_msg_header, err_msg]

        stack = VStack(stack_lst, expand=True)
        frame = Frame(stack, title= title, expand=True)
        if as_str:
            temp = frame | to_rich_str
            return temp
        else:
            frame | show
            return ""
    
    except Exception as exc:
        import traceback
        print(f"!Visual Error output failed!")
        try:
            print(type(exc))
            print(exc)
            print(traceback.format_exc())
        except:
            pass
        try:
            return zef_ui_err_fallback(err)
        except:
            pass


# This class exists solely to enable printing of the _ErrorType class above in
# normal exception handling. It also allows us to identify points at which we
# have ended control in the evaluation engine, and had to throw to the external
# environment.
class ExceptionWrapper(Exception):
    def __init__(self, wrapped):
        self.wrapped = wrapped

# When to use an _ErrorType or a ExceptionWrapper?
#
# Inside of evaluation-engine control, _ErrorType should be always present. This
# means whenever _ErrorTypes are present, no more traceback information of code
# is gathered. Implementation functions take want to attach metadata and
# otherwise say "my child broke, not me" should catch all other exceptions and
# turn them into _ErrorTypes.
#
# ExceptionWrappers are on the same level as regular python exceptions, and
# should only be thrown when we are giving up control back to regular python
# execution (which could be to outermost-scope or in the execution of a user
# zef-function).
#
# The only difference between an ExceptionWrapper and an Exception is that we
# (as the evalution engine) know that we can unwrap an ExceptionWrapper as
# exc.wrapped to get an _ErrorType and continue propagate that internally if we
# want.
    


#     def __new__(cls, *args):
#         return cls.BasicError(*args)

#     def __repr__(self):
#         return f'Error'

predefined_errors = [
    "TypeError",
    "RuntimeError",
    "ValueError",
    "NotImplementedError",
    "BasicError",
    "UnexpectedError",
    "MapError",
    "MatchError",
    "Panic",
]

def error_dir(self):
    return predefined_errors
def error_getattr(self, x):
    return Error_(x)
def error_ctor(*args):
    return Error.BasicError(*args)
    

from .VT import make_VT
Error = make_VT("Error",
                pytype=Error_,
                attr_funcs=(error_getattr, None, error_dir),
                constructor_func=error_ctor)

class EvalEngineCoreError(Exception):
    def __init__(self, exc):
        self.name = "EvalEngineCoreError"
        self.nested, self.frames = convert_python_exception(exc)

    def __str__(self):
        return f"This is a failure in the core evaluation engine!"#, {self.nested['type']}: {self.nested['args']}"





# Error spec

# Error = PythonType[Exception] & Object[Pattern[{
#     # Type name
#     "name": String,
#     # User added details. For generic cases is empty (e.g. UnexpectedError()
#     "args": List[Any],
#     # Error that caused this error
#     "nested": Error | PythonExcInfo | Null,
#     # Context information, up until the nested error
#     "contexts": List[Context],
# }]]

# PythonExcInfo = Pattern[{
#     # Python exception class name
#     "type": String,
#     # Contents of the exception
#     "args": List[Any],
#     # If the python exception was nested using python raising syntax.
#     "nested": Tuple[PythonExcInfo, List[FrameInfo]],
# }]

# Context = OpContext | FramesContext | MetadataContext

# OpContext = OpChainContext & Optional[ActiveOpContext | CollectingGeneratorContext]
# OpChainContext = Pattern[{
#     "chain": LazyValue & With[CollectingOp | ForEachingOp],
# }]
# # When the evaluation engine gets an error after running a particular op's
# # implementation function.
# ActiveOpContext = Pattern[{
#     # Index of the op in the chain that is being run
#     "op_i": Int,
#     # Input to this op
#     "input": Any,
# }]
# # When the evaluation engine is collecting up a ZefGenerator at the end of the
# # chain.
# CollectingGeneratorContext = Pattern[{
#     "state": "collecting",
#     # Index of the generator
#     "val_i": Int,
# }]

# FramesContext = Pattern[{
#     "frames": List[FrameInfo]
# }]
# FrameInfo = Pattern[{
#     "lineno": Int,
#     "filename": String,
#     "func_name": String,
#     "locals": Dict[String,Any],
# }]

# # This is a generic "anyone can hook in" context. Example, used by map to add
# # the context of the last input.
# MetadataContext = Pattern[{
#     "metadata": Any,
# }]
    

# * Manipulating errors

def add_error_context(error, context):
    return prepend_error_contexts(error, [context])
def prepend_error_contexts(error, contexts):
    # from copy import copy
    err = Error_(error.name)
    err.args = error.args
    err.contexts = error.contexts
    err.nested = error.nested
    out = err
    if getattr(out, "contexts", None) is None:
        out.contexts = []

    out.contexts = [*contexts, *out.contexts]

    return out


# * Displaying errors

def str_tb_up_to_zef(tb):
    import traceback
    l = filter_tb_to_before_zef(tb)
    return traceback.format_list(l)

def str_frame_info(frames):
    if len(frames) == 0:
        return "-- EMPTY (need to hide this in printing)"
    s = []
    for frame in reversed(frames):
        s.append(f"-- Frame: in func({frame['func_name']}) in '{frame['filename']}'@{frame['lineno']}")
    return '\n'.join(s)

def str_zef_error(error):
    s = ""
    if type(error) == Error_:
        if error.nested is not None:
            s += str_zef_error(error.nested)
            s += "\n^^^^^\nCaused "

        s += error.name + ": "
        if len(error.args):
            s += str(error.args)
    elif type(error) == EvalEngineCoreError:
        s += "CORE EVALUATION ENGINE ERROR!"
        s += "CORE EVALUATION ENGINE ERROR!"
        s += "CORE EVALUATION ENGINE ERROR!"
        import traceback
        s += '\n'.join(traceback.format_exception(error))
        # s += str_python_exception_dict(exc_info)
    elif type(error) == dict:
        s += str_python_exception_dict(error)
    else:
        s += f"Don't know how to print error of type {type(error)}"
        s += str(error)

    # Now make the context/traceback up
    for context in reversed(getattr(error, "contexts", [])):
        try:
            s += '\n' + zef_error_context_str(context)
        except Exception as exc:
            s += "\ncontext couldn't be printed"
            s += " " + str(exc)

    s += "\n==============================================="
    return s

def str_truncate(s):
    # return s
    if len(s) > 30:
        s = s[:15] + " ... " + s[-15:]
    return s


def str_op_chain(op):
    from . import RT
    if op[0] == RT.Function:
        func = op[1][0][1]
        import types
        if isinstance(func, types.FunctionType):
            name = func.__name__
        else:
            name = str(func)
        return f"Function: {name}"
    elif op[0] == RT.Apply:
        return f"Apply: {str_op_chain(op[1][0].el_ops[0])}"
    else:
        try:
            return str_truncate(str(op))
        except:
            return "ERROR PRINTING OP"

def zef_error_context_str(context):
    s = ""

    if "metadata" in context:
        l = []
        for key,val in context["metadata"].items():
            l += [f"@@@ '{key}' = '{val}'"]
        s += '\n'.join(l)

    if "frames" in context:
        s += str_frame_info(context["frames"])
        return s

    if "op" in context:
        # op_s = str_truncate(str(op_chain_pretty_print(context['op'])))
        s += f" evaling op {str_op_chain(context['op'])}"
    if "input" in context:
        try:
            input_s = str_truncate(str(context['input']))
        except:
            input_s = "ERROR PRINTING INPUT"
        s += f" using input ({input_s})"
    if "chain" in context:
        try:
            chain_s = str_truncate(str(context['chain']))
        except:
            chain_s = "ERROR PRINTING CHAIN"
        s += f" of chain ({chain_s})"
    if "state" in context:
        if context["state"] == "generator":
            s += f" up to i={context['i']} in generator"
            if context['i'] > 0:
                s += f" and last output was {context['last_output']}"

    if s == "":
        s = "WHY IS THIS EMPTY???"
        s += "\n" + str(context)
    return s
    
def str_python_exception_dict(d):
    s = ""
    s += f"{d['type']} - {d['args']}"
    # str_frame_info(d['frames'])
    if "nested" in d:
        nested_d,frames = d["nested"]
        s += "\n++++" + str_python_exception_dict(nested_d)
        s += "\n++++" + str_frame_info(frames)
    return s
    

# * Utils

def filter_tb_to_before_zef(tb):
    import traceback
    # Want to avoid frames that first enter the zef codebase
    l = traceback.extract_tb(tb)
    for i,item in enumerate(l):
        if "op_structs.py" in item.filename:
            break
    l = l[:i]
    return l

def convert_python_exception(exc, filter_frames=True):
    tb = exc.__traceback__
    frames = process_python_tb(tb, filter_frames)


    out = {
        "type": str(type(exc)),
        # Might need to serialize this?
        "args": exc.args,
    }

    if exc.__cause__ is not None:
        out["nested"] = convert_python_exception(exc.__cause__)
    elif exc.__context__ is not None and not exc.__suppress_context__:
        out["nested"] = convert_python_exception(exc.__context__)

    return out, frames

def process_python_tb(tb, filter=True):
    # Not sure if ops are available here so doing this in pure python
    frames = []
    while tb is not None:
        frames.append(tb.tb_frame)
        tb = tb.tb_next
    # Get rid of the first one - at least where this is currently called from we don't
    # want to include "evaluate". In the future, maybe this will have to be a switch.
    # frames = frames[1:]
    def process_frame(frame):
        return {
            "locals": frame.f_locals,
            "lineno": frame.f_lineno,
            "filename": frame.f_code.co_filename,
            "func_name": frame.f_code.co_name,
        }

    frame_info = [process_frame(frame) for frame in frames]

    if filter:
        frame_info = [frame for frame in frame_info if "op_structs.py" not in frame["filename"]]

    return frame_info


def make_custom_error(e, err_type, err_message, context):
    py_e,frames = convert_python_exception(e)
    err_instance = err_type
    if err_message: py_e['args'] = ((err_message, ))
    err_instance.nested = py_e
    err_instance = add_error_context(err_instance, {"frames": frames})
    return add_error_context(err_instance, context)
