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







