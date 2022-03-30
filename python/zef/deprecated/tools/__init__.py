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

from ._others import *

# from .serialization import *
from .time import *
# from .zascii import *
# from .deltas import *

__all__ = [
    "text_art",
    "catch_and_replace",
] + (time.__all__ +
     # zascii.__all__ +
     # deltas.__all__ +
     _others.__all__)



# --------------------------------------------------------------------------------------------------------------------------------------------




def text_art(s: str) -> str:
    from art import text2art        
    def add_comment(s: str) -> str:
        v = (s.split('\n'))[:-1] | ops.map[lambda ro: '#  '+ro]
        return '\n'.join(v)
    
    s2 = s.replace(' ', '   ')
    return add_comment(text2art(s2))



def catch_and_replace(fct, alternative=None):
    """if the evaluating function throws, use the alternative value"""
    def wrapped(x):
        try:
            return fct(x)
        except:
            return None            
    return wrapped
