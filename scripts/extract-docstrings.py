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
