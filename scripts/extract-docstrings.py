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

from inspect import getdoc
from zefdb.lazy_zefops import *
from zefdb.lazy_zefops.dispatch_dictionary import _op_to_functions

"""
def get_docstring(p: tuple):
    label, (imp_fct, tp_fct) = p
    value = getdoc(imp_fct[0])
    return {label: label, value: value}
"""

markdown = """---
id: zef-ops
title: ZefOps
author: bot
---

# ZefOps
"""

for key, value in _op_to_functions.items():
    markdown += f"### {key}\n```python\n{getdoc(value[0])}\n```\n\n"

filehandle = open('zef-ops.mdx', 'w')
filehandle.write(markdown)
filehandle.close()
print(f"file zef-ops.mdx written to disk")
