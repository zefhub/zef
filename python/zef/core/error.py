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

class _ErrorType(Exception):
    def __set_name__(self, parent, name):
        self.name = name

    def __call__(self, *args):
        err = _ErrorType()
        err.name = self.name
        err.args = args
        return err

    def __repr__(self):
        if hasattr(self, "contexts"):
            return str_zef_error(self)
        
        if not self.args or len(self.args) == 0: args = "()"
        elif len(self.args) == 1: args = f"({repr(self.args[0])})"
        else: args = self.args
        return f'{self.name}{args}'

    def __eq__(self, other):
        if not isinstance(other, _ErrorType): return False
        return self.name == other.name and self.args == other.args

    def __bool__(self):
        return False
    

class _Error:
    TypeError    = _ErrorType()
    RuntimeError = _ErrorType()
    ValueError   = _ErrorType()
    NotImplementedError = _ErrorType()
    BasicError = _ErrorType()
    UnexpectedError = _ErrorType()
    MapError = _ErrorType()
    Panic = _ErrorType()

    def __call__(self, *args):
        return self.BasicError(*args)

    def __repr__(self):
        return f'Error'


Error = _Error()

class EvalEngineCoreError(Exception):
    def __init__(self, exc):
        self.exc_data = convert_python_exception(exc)

    def __str__(self):
        return f"This is a failure in the core evaluation, {self.exc_data['type']}: {self.exc_data['args']}"


# * Manipulating errors

def add_error_context(error, context):
    return prepend_error_contexts(error, [context])
def prepend_error_contexts(error, contexts):
    from copy import copy
    out = copy(error)
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
    s = []
    for frame in reversed(frames):
        s.append(f"-- Frame: in func({frame['func_name']}) in '{frame['filename']}'@{frame['lineno']}")
    return '\n'.join(s)

def str_zef_error(error):
    s = ""
    if type(error) == _ErrorType:
        if error.name == "Panic":
            # THe arg is either a wrapper ErrorType or a python exception
            if len(error.args) > 1:
                print("WARNING DON'T KNOW HOW TO INTERPRET MORE THAN ONE ARG FOR A PANIC")
                print("WARNING DON'T KNOW HOW TO INTERPRET MORE THAN ONE ARG FOR A PANIC")
                print("WARNING DON'T KNOW HOW TO INTERPRET MORE THAN ONE ARG FOR A PANIC")
                print("WARNING DON'T KNOW HOW TO INTERPRET MORE THAN ONE ARG FOR A PANIC")
                print("WARNING DON'T KNOW HOW TO INTERPRET MORE THAN ONE ARG FOR A PANIC")
                print(error.args[1:])
            arg = error.args[0]
            if type(arg) == _ErrorType:
                s += str_zef_error(arg)
            elif type(arg) == dict:
                s += str_python_exception_dict(arg)
            s += "\n^^^^^\nCaused Panic"
        else:
            arg = error.args[0]
            if type(arg) == _ErrorType:
                s += str_zef_error(arg)
            elif type(arg) == dict:
                s += str_python_exception_dict(arg)
            s += f"\n^^^^^\nCaused {error.name}"
            if type(arg) not in [_ErrorType, dict]:
                print_args = error.args
            else:
                print_args = error.args[1:]
            if len(print_args) > 0:
                s += ": " + str(print_args)
                
    elif type(error) == EvalEngineCoreError:
        s += "CORE EVALUATION ENGINE ERROR!"
        s += "CORE EVALUATION ENGINE ERROR!"
        s += "CORE EVALUATION ENGINE ERROR!"
        import traceback
        s += '\n'.join(traceback.format_exception(error))
        # s += str_python_exception_dict(exc_info)
    else:
        s = str(error)
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
            # for loc in context["chain"].construction_locs:
            #     chain_s += f"\nLocation: {loc['filename']} @ {loc['lineno']}"
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
        s += "\n++++" + str_python_exception_dict(d["nested"])
        s += "\n++++" + str_frame_info(d["nested"]["frames"])
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

def convert_python_exception(exc):
    tb = exc.__traceback__
    frames = process_python_tb(tb)
        

    out = {
        "type": str(type(exc)),
        # Might need to serialize this?
        "args": exc.args,
        "frames": frames,
    }

    if exc.__context__ is not None:
        out["nested"] = convert_python_exception(exc.__context__)
    elif exc.__cause__ is not None:
        out["nested"] = convert_python_exception(exc.__cause__)

    return out

def process_python_tb(tb, filter=True):
    # Not sure if ops are available here so doing this in pure python
    frames = []
    while tb is not None:
        frames.append(tb.tb_frame)
        tb = tb.tb_next

    # Get rid of the first one - at least where this is currently called from we don't
    # want to include "evaluate". In the future, maybe this will have to be a switch.
    frames = frames[1:]

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
