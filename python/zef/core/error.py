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

class _ErrorType():
    def __set_name__(self, parent, name):
        self.name = name

    def __call__(self, *args):
        err = _ErrorType()
        err.name = self.name
        err.args = args
        return err

    def __repr__(self):
        if not self.args or len(self.args) == 0: args = "()"
        elif len(self.args) == 1: args = f"({repr(self.args[0])})"
        else: args = self.args
        return f'{self.name}{args}'

    def __eq__(self, other):
        if not isinstance(other, _ErrorType): return False
        return self.name == other.name and self.args == other.args
    

class _Error:
    TypeError    = _ErrorType()
    RuntimeError = _ErrorType()
    ValueError   = _ErrorType()
    NotImplementedError = _ErrorType()
    BasicError = _ErrorType()

    def __call__(self, *args):
        return self.BasicError(*args)

    def __repr__(self):
        return f'Error'



Error = _Error()

