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

    def __iter__(self):
        return self.generator_fct()

from .VT import make_VT
make_VT("ZefGenerator", pytype=ZefGenerator_)