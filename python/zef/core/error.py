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

class Error_():
    def __init__(self, name):
        self.name = name

    # def __set_name__(self, parent, name):
    #     self.name = name

    def __call__(self, *args):
        err = Error_(self.name)
        err.args = args
        return err

    def __repr__(self):
        if not self.args or len(self.args) == 0: args = "()"
        elif len(self.args) == 1: args = f"({repr(self.args[0])})"
        else: args = self.args
        return f'{self.name}{args}'

    def __eq__(self, other):
        if not isinstance(other, Error_): return False
        return self.name == other.name and self.args == other.args

    def __bool__(self):
        return False
    

# class _Error:
#     TypeError    = _ErrorType()
#     RuntimeError = _ErrorType()
#     ValueError   = _ErrorType()
#     NotImplementedError = _ErrorType()
#     BasicError = _ErrorType()

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

