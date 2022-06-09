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

from ._core import RT
from .op_structs import  evaluating, LazyValue, Awaitable, ZefOp, CollectingOp, SubscribingOp, ForEachingOp

# The dictionary of registered zefops. A RT -> (Func, Func) mapping.
_dispatch = {}

def zefop(func=None, *, name=None, rt=None):
    if name is None:
        # Not sure how to get this any other way
        op_name = func.__name__
    else:
        op_name = name

    from .pure_utils import to_pascal_case
    if rt is None:
        rt = RT(to_pascal_case(op_name))

    if rt not in _dispatch:
        globals()[op_name] = ZefOp(((rt, ()),))
        _dispatch[rt] = (func, None)
    else:
        assert _dispatch[rt][0] is None, f"Trying to overwrite implementation of zefop {op_name}"
        assert func is not None, f"Trying to reprototype zefop {op_name}"
        _dispatch[rt] = (func, _dispatch[rt][1])
    return func

def add_tp(func, *, name=None, rt=None):
    if name is None:
        # Not sure how to get this any other way
        op_name = func.__name__
    else:
        op_name = name

    from .pure_utils import to_pascal_case
    if rt is None:
        rt = RT(to_pascal_case(op_name))

    assert rt in _dispatch, f"No implementation to add type information to: {op_name}"
    assert _dispatch[rt][1] is None, f"Trying to overwrite type information for {op_name}"
    _dispatch[rt] = (_dispatch[rt][0], func)

    # By default return the function implementation, so that we don't overwrite
    # a previously declared implementation
    return _dispatch[rt][0]


for_each        = ForEachingOp(ZefOp(()))
collect         = CollectingOp(ZefOp(()))
subscribe       = SubscribingOp(ZefOp(()))

zefop(name="run")
# For some reason we need to curry in something here
run             = ZefOp(((RT.Run, (evaluating,)), ))

zefop(name="function")

zefop(name="cycle")
zefop(name="repeat")
zefop(name="contains")
zefop(name="contained_in")
zefop(name="skip")
zefop(name="all")
zefop(name="any")
zefop(name="slice")                 # lowercase slice is used in core Python
zefop(name="join")
zefop(name="trim")
zefop(name="trim_left")
zefop(name="trim_right")
zefop(name="yo")
zefop(name="tap")
zefop(name="push")

zefop(name="cartesian_product")
zefop(name="permutations")
zefop(name="combinations")
zefop(name="sum")
zefop(name="product")
zefop(name="add")
zefop(name="subtract")
zefop(name="multiply")
zefop(name="divide")
zefop(name="mean")
zefop(name="variance")
zefop(name="power")
zefop(name="exponential")
zefop(name="logarithm")

zefop(name="equals")
zefop(name="greater_than")
zefop(name="less_than")
zefop(name="greater_than_or_equal")
zefop(name="less_than_or_equal")
zefop(name="Not")
zefop(name="And")
zefop(name="Or")
zefop(name="xor")

zefop(name="always")
zefop(name="docstring")
zefop(name="source_code")

zefop(name="absorbed")
zefop(name="without_absorbed")

zefop(name="get_in")
zefop(name="insert_in")
zefop(name="remove_in")
zefop(name="update_in")
zefop(name="update_at")
zefop(name="insert_at")
zefop(name="update")
zefop(name="remove_at")
zefop(name="merge")
zefop(name="merge_with")
zefop(name="int_to_alpha")
zefop(name="permute_to")


# Implemented Lazy ZefOps
zefop(name="expect")
zefop(name="filter")
zefop(name="without")
zefop(name="select_keys")
zefop(name="modulo")
zefop(name="select_by_field")
zefop(name="apply_functions")
zefop(name="map")
zefop(name="map_cat")
zefop(name="identity")
zefop(name="concat")
zefop(name="zip")
zefop(name="prepend")
zefop(name="append")
zefop(name="interleave")
zefop(name="interleave_longest")
zefop(name="chunk")
zefop(name="sliding")
zefop(name="stride")
zefop(name="insert")
zefop(name="insert_into")
zefop(name="reverse_args")
zefop(name="remove")
zefop(name="get")
zefop(name="get_field")
zefop(name="enumerate")
zefop(name="items")
zefop(name="values")
zefop(name="keys")
zefop(name="reverse")
zefop(name="reduce")
zefop(name="iterate")
zefop(name="scan")
zefop(name="group_by")
zefop(name="transpose")
zefop(name="frequencies")
zefop(name="max")
zefop(name="min")
zefop(name="max_by")
zefop(name="min_by")
zefop(name="first")
zefop(name="second")
zefop(name="last")
zefop(name="single")           
zefop(name="single_or")
zefop(name="only")                 # TODO: retire, since we renamed this to 'single'
zefop(name="take")
zefop(name="take_while")
zefop(name="take_while_pair")
zefop(name="take_until")                 # TODO: use 'take_until' in the RX-sense: complete the stream based on another stream emitting an item. Add 'including' to take_while as a flag for the current behavior?
zefop(name="skip_while")
zefop(name="length") 
zefop(name="nth") 
zefop(name="now") 
zefop(name="events") 
zefop(name="preceding_events") 
zefop(name="to_delegate") 
zefop(name="delegate_of") 
zefop(name="target") 
zefop(name="source") 
zefop(name="L")
zefop(name="O")
zefop(name="Z")
zefop(name="RAE")
zefop(name="time") 
zefop(name="value")
zefop(name="sort")
zefop(name="uid")
zefop(name="frame")
zefop(name="discard_frame")
zefop(name="to_frame")                           # TODO: retire this. Use 'in_frame' instead
zefop(name="in_frame")
zefop(name="to_graph_slice")
zefop(name="to_tx")
zefop(name="time_travel")
zefop(name="next_tx")             
zefop(name="previous_tx")     
zefop(name="to_ezefref")
zefop(name="root")
zefop(name="terminate") 
zefop(name="assign") 
zefop(name="is_a")
zefop(name="is_represented_as")
zefop(name="representation_type")
zefop(name="rae_type")
zefop(name="abstract_type")
zefop(name="fill_or_attach")
zefop(name="Assert")
zefop(name="tag")
zefop(name="untag")
zefop(name="sync")
zefop(name="to_clipboard")
zefop(name="from_clipboard")
zefop(name="text_art")

zefop(name="sign")
zefop(name="attempt")
zefop(name="bypass")
zefop(name="pattern")
zefop(name="replace")
zefop(name="distinct")
zefop(name="distinct_by")
zefop(name="is_distinct")
zefop(name="is_distinct_by")
zefop(name="shuffle")
zefop(name="split")
zefop(name="split_if")
zefop(name="graphviz")

zefop(name="schema")
zefop(name="exists_at")
zefop(name="base_uid")
zefop(name="origin_uid")
zefop(name="origin_rae")

zefop(name="has_out")                # z1 | has_out[RT.Foo] use  (z1, RT.Foo, Z) | exists  /   (z, RT.Foo, RAE) | exists[g]  /   (z, RT.Foo, RAE) | exists[now(g)][single]
zefop(name="has_in")                 # z1 | has_in[RT.Foo]  use  (Z, RT.Foo, z1) | exists

zefop(name="In")
zefop(name="Ins")
zefop(name="Out")
zefop(name="Outs")
zefop(name="in_rel")
zefop(name="in_rels")
zefop(name="out_rel")
zefop(name="out_rels")


zefop(name="is_zefref_promotable")  # Retire this. this is a old love level operator. We can use is_a[RAE] or an extended concept new.
zefop(name="time_slice")        
zefop(name="graph_slice_index") 
    
zefop(name="instantiation_tx")       # use tx[instantiated]
zefop(name="termination_tx")         # use tx[terminated]   
zefop(name="relations")             # g | now | all[(z1, RT.Bar, z2)]   with pattern matching style any of the three args can also be replaced with a more general class
zefop(name="relation")              # looking through our code base for use cases of this op, I don't think a separate operator is necessary. Just use the syntax above followed by ... | single. If required more often, it is much easier to add this in future than to remove it.
zefop(name="unpack")
zefop(name="_any", rt=RT._Any)
zefop(name="has_relation")     

zefop(name="replace_at")           
zefop(name="pad_left")           
zefop(name="pad_right")           
zefop(name="pad_center")           
zefop(name="ceil")           
zefop(name="floor")           
zefop(name="round")           
zefop(name="random_pick")           

zefop(name="to_json")           
zefop(name="from_json")   

zefop(name="to_yaml")
zefop(name="from_yaml")

zefop(name="to_toml")
zefop(name="from_toml")

zefop(name="to_csv")
zefop(name="from_csv")

zefop(name="read_file")
zefop(name="load_file")
zefop(name="write_file")
zefop(name="save_file")


zefop(name="pandas_to_gd")

zefop(name="to_pipeline")
zefop(name="inject")
zefop(name="inject_list")


zefop(name="is_alpha")
zefop(name="is_numeric")
zefop(name="is_alpha_numeric")
zefop(name="to_upper_case")
zefop(name="to_lower_case")

zefop(name="to_pascal_case")
zefop(name="to_camel_case")
zefop(name="to_kebab_case")
zefop(name="to_snake_case")
zefop(name="to_screaming_snake_case")


zefop(name="make_request")


zefop(name="zascii_to_asg")
zefop(name="zascii_to_schema")

                # Syntax????????????????? 
                # has_relation(z1, RT.Foo, z2)      replaced by 
                #       1) (z1, RT.Foo, z2) | exists[g_slice]   or 
                #       1) (z1, RT.Foo, z2) | contained_in[g_slice]   or 
                #       2) g_slice | contains[(z1, RT.Foo, z2)]
# Syntax choices:   
#       exists or contained_in?
#       All or instances?    Also: my_delegate | all    or my_delegate | instances?





zefop(name="blake3")
zefop(name="value_hash")        
zefop(name="to_zef_list")
zefop(name="transact")
# transact

# subscribe
# keep_alive
# incoming
# outgoing
# on_instantiation
# on_termination
# on_value_assignment


zefop(name="peel")                
zefop(name="match")                
zefop(name="Range")      
zefop(name="zstandard_compress")
zefop(name="zstandard_decompress")
zefop(name="to_bytes")
zefop(name="utf8bytes_to_string")
zefop(name="base64string_to_bytes")
zefop(name="bytes_to_base64string")
zefop(name="is_between")
zefop(name="If")

zefop(name="field")
zefop(name="fields")
zefop(name="apply")
zefop(name="split_on_next")
zefop(name="indexes_of")



zefop(name="examples")
zefop(name="signature")
zefop(name="tags")
zefop(name="related_ops")
zefop(name="operates_on")
zefop(name="used_for")


# TODO: implement
zefop(name="on")         


          
# match
# split_before
# split_after
# split_at
# split_when



# delay
# window(Max[10], Max[30/sec, over[2*sec]])
# 
# time_travel         # RuntimeError: Only(EZefRefs zs) request, but length was 0





# -------- These are not ZefOps, but using the `.` operator, they return ZefOps.
# The user may not even be aware of this distinction and therefore this namespace 
# is the most natural to put them in.
class FClass:    
    def __getattr__(self, s: str):        
        return field[RT(s)]    # just returns a normal zefop called 'field'

F = FClass()

class FsClass:    
    def __getattr__(self, s: str):        
        return fields[RT(s)]    # just returns a normal zefop called 'field'

Fs = FsClass()


from .abstract_raes import make_custom_entity

please_instantiate = make_custom_entity(name_to_display='please_instantiate', predetermined_uid='783320c1c3de2610')
please_terminate   = make_custom_entity(name_to_display='please_terminate', predetermined_uid='67cb88b71523f6d9')
please_assign      = make_custom_entity(name_to_display='please_assign',    predetermined_uid='4d4a93522f75ed21')

allow_tombstone    = make_custom_entity(name_to_display='allow_tombstone', predetermined_uid='6438364576748387')

instantiated     = make_custom_entity(name_to_display='instantiated', predetermined_uid='60252a53a03086b7')
terminated       = make_custom_entity(name_to_display='terminated', predetermined_uid='4f676154ffeb9dc8')
assigned         = make_custom_entity(name_to_display='assigned', predetermined_uid='c31287dab677f38c')

infinity           = make_custom_entity(name_to_display='infinity',    predetermined_uid='4906648460291096')
nil                = make_custom_entity(name_to_display='nil',         predetermined_uid='1654670075329719') #| register_call_handler[f1] | run[execute] | get['entity'] | collect  # TODO


