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

#%%

from zefdb import *
from zefdb.zefops import *


g = Graph()


@zef_function(g)
def my_f(x):
   return x+1



@zef_function(g, label='ulfs favorite function')
def some_fct(x):
    """a little test function2"""
    def gg(y):
        return y+1
    return x*x+2*gg(x)


@zef_function(g)
def my_f2(s, n_rep:int):
    assert isinstance(s, str)
    return s*n_rep



#%%


z_fct = g | instances[now][ET.ZEF_Function] | last
execute[z_fct]('abc', 2)

#%%%%

g | instances[now][ET.ZEF_Function]  | to_clipboard


#%%%%







