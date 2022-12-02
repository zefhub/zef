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

from .VT import *
from ._error import *
class ZefGenerator_:
    """
    A class that acts as a uniform interface instead of Python's
    builtin generator. Defining a regular generator using yield
    in Python does not always obey value semantics. It still keeps
    state when iterated over.
    
    Example:
    >>> def repeat(x, n=None):    
    >>>  def make_wrapper():
    >>>      if n is None:
    >>>          while True:
    >>>              yield x
    >>>      else:
    >>>             for _ in range(n):
    >>>                 yield x
    >>>  
    >>>     return ZefGenerator(make_wrapper)

    Returning `wrapper()` directly is equivalent to the class
    where `__iter__` returns self, thus maintaining state from
    previous iterations.
    """
    def __init__(self, generator_fct):
        self.generator_fct = generator_fct
        self.contexts = []

    def __iter__(self):
        from .op_implementations.implementation_typing_functions import wrap_error_raising
        # return self.generator_fct()
        def wrap_errors():
            it = iter(self.generator_fct())
            i = 0
            last_output = None
            while True:
                cur_context = [*self.contexts,
                               {"state": "generator",
                                "i": i,
                                "last_output": last_output}]
                try:
                    item = next(it)
                    yield item
                    i += 1
                    last_output = item
                except StopIteration:
                    return
                except GeneratorExit:
                    raise
                except Exception as e:
                    if not custom_error_handling_activated():
                        raise
                    elif isinstance(e, EvalEngineCoreError):
                        wrap_error_raising(e, cur_context)
                    elif isinstance(e, ExceptionWrapper):
                        wrap_error_raising(e, cur_context)
                    elif isinstance(e, Error_):
                        wrap_error_raising(e, cur_context)
                    else:
                        wrap_error_raising(e, cur_context)
        return wrap_errors()

    def __str__(self):
        return "ZefGenerator"

    def add_context(self, context):
        new_gen = ZefGenerator_(self.generator_fct)
        new_gen.contexts = [context, *self.contexts]
        return new_gen

from .VT import make_VT
make_VT("ZefGenerator", pytype=ZefGenerator_)