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

from operator import ne
from ._ops import *
from dataclasses import dataclass
from . import VT


@dataclass
class Val:
    arg: VT.Any

class FlatGraph:
    def __init__(self, *args):
        if args == ():
            self.key_dict = {}
            self.blobs = ()
        elif len(args) == 1 and isinstance(args[0], list):
            new_fg = FlatGraph()
            new_fg = new_fg | insert[args[0]] | collect
            self.key_dict = new_fg.key_dict
            self.blobs = new_fg.blobs
        else:
            raise NotImplementedError("FlatGraph with args")

    def __repr__(self):
        kdict = "\n".join([f"({k}=>{v})" for k,v in self.key_dict.items()])
        blobs = "\n".join([str(e) for e in self.blobs])
        return f'FlatGraph(\n{kdict}\n-------\n{blobs}\n)'
    
    def __or__(self, other):
        return LazyValue(self) | other

    def __getitem__(self, key):
        return get(self, key)


class FlatRef:
    def __init__(self, fg, idx):
        self.fg = fg
        self.idx = idx
    def __repr__(self):
        return f'<FlatRef #{abs(self.idx)} {repr(self.fg.blobs[self.idx][1])}>'
    
    def __or__(self, other):
        return LazyValue(self) | other

    def __gt__(self, other):
        return LazyValue(self) > other

    def __lt__(self, other):
        return LazyValue(self) < other

    def __lshift__(self, other):
        return LazyValue(self) << other
    
    def __rshift__(self, other):
        return LazyValue(self) >> other

class FlatRefs:
    def __init__(self, fg, idxs):
        self.fg = fg
        self.idxs = idxs
    def __repr__(self):
        newline = '\n'
        return f"""<FlatRefs len={len(self.idxs)}> [
{["    " + repr(FlatRef(self.fg, i)) for i in self.idxs] | join[newline] | collect}
]"""
    
    def __or__(self, other):
        return LazyValue(self) | other

    def __iter__(self):
        return (FlatRef(self.fg, i) for i in self.idxs)

    def __gt__(self, other):
        return LazyValue(self) > other

    def __lt__(self, other):
        return LazyValue(self) < other

    def __lshift__(self, other):
        return LazyValue(self) << other
    
    def __rshift__(self, other):
        return LazyValue(self) >> other