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

def register_zefop(rt, imp, tp):
    from .dispatch_dictionary import _op_to_functions
    import zef
    import re

    assert(type(rt) == zef.pyzef.main.RelationType)

    op_name = '_'.join(re.findall('[A-Z][^A-Z]*', str(rt))).lower()
    op = ZefOp(((rt, ()),))
    globals()[op_name] = op
    _op_to_functions[rt] = (imp, tp)
    return op


for_each        = ForEachingOp(ZefOp(()))
collect         = CollectingOp(ZefOp(()))
subscribe       = SubscribingOp(ZefOp(()))

run             = ZefOp(((RT.Run, (evaluating,)), ))

def make_zefop(rt):
    return ZefOp(((rt, ()), ))

cycle           = make_zefop(RT.Cycle)
repeat          = make_zefop(RT.Repeat)
contains        = make_zefop(RT.Contains)
contained_in    = make_zefop(RT.ContainedIn)
skip            = make_zefop(RT.Skip)
all             = make_zefop(RT.All)
any             = make_zefop(RT.Any)
slice           = make_zefop(RT.Slice)                 # lowercase slice is used in core Python
join            = make_zefop(RT.Join)
trim            = make_zefop(RT.Trim)
trim_left       = make_zefop(RT.TrimLeft)
trim_right      = make_zefop(RT.TrimRight)
yo              = make_zefop(RT.Yo)
tap             = make_zefop(RT.Tap)
push            = make_zefop(RT.Push)

cartesian_product = make_zefop(RT.CartesianProduct)
permutations    = make_zefop(RT.Permutations)
combinations    = make_zefop(RT.Combinations)
sum             = make_zefop(RT.Sum)
product         = make_zefop(RT.Product)
add             = make_zefop(RT.Add)
subtract        = make_zefop(RT.Subtract)
multiply        = make_zefop(RT.Multiply)
divide          = make_zefop(RT.Divide)
mean            = make_zefop(RT.Mean)
variance        = make_zefop(RT.Variance)
power           = make_zefop(RT.Power)
exponential     = make_zefop(RT.Exponential)
logarithm       = make_zefop(RT.Logarithm)

equals          = make_zefop(RT.Equals)
greater_than    = make_zefop(RT.GreaterThan)
less_than       = make_zefop(RT.LessThan)
greater_than_or_equal = make_zefop(RT.GreaterThanOrEqual)
less_than_or_equal = make_zefop(RT.LessThanOrEqual)
Not             = make_zefop(RT.Not)
And             = make_zefop(RT.And)
Or              = make_zefop(RT.Or)
xor             = make_zefop(RT.Xor)

always          = make_zefop(RT.Always)
docstring       = make_zefop(RT.Docstring)
source_code     = make_zefop(RT.SourceCode)

absorbed        = make_zefop(RT.Absorbed)
without_absorbed= make_zefop(RT.WithoutAbsorbed)

get_in          = make_zefop(RT.GetIn)
insert_in       = make_zefop(RT.InsertIn)
remove_in       = make_zefop(RT.RemoveIn)
update_in       = make_zefop(RT.UpdateIn)
update_at       = make_zefop(RT.UpdateAt)
insert_at       = make_zefop(RT.InsertAt)
update          = make_zefop(RT.Update)
remove_at       = make_zefop(RT.RemoveAt)
merge           = make_zefop(RT.Merge)
merge_with      = make_zefop(RT.MergeWith)
int_to_alpha    = make_zefop(RT.IntToAlpha)
permute_to      = make_zefop(RT.PermuteTo)


# Implemented Lazy ZefOps
expect          = make_zefop(RT.Expect)
filter          = make_zefop(RT.Filter)
select_keys     = make_zefop(RT.SelectKeys)
modulo          = make_zefop(RT.Modulo)
select_by_field = make_zefop(RT.SelectByField)
apply_functions = make_zefop(RT.ApplyFunctions)
map             = make_zefop(RT.Map)
identity        = make_zefop(RT.Identity)
concat          = make_zefop(RT.Concat)
zip             = make_zefop(RT.Zip)
prepend         = make_zefop(RT.Prepend)
append          = make_zefop(RT.Append)
interleave      = make_zefop(RT.Interleave)
interleave_longest = make_zefop(RT.InterleaveLongest)
chunk           = make_zefop(RT.Chunk)
sliding         = make_zefop(RT.Sliding)
stride          = make_zefop(RT.Stride)
insert          = make_zefop(RT.Insert)
insert_into     = make_zefop(RT.InsertInto)
reverse_args    = make_zefop(RT.ReverseArgs)
remove          = make_zefop(RT.Remove)
get             = make_zefop(RT.Get)
get_field       = make_zefop(RT.GetField)
enumerate       = make_zefop(RT.Enumerate)
items           = make_zefop(RT.Items)
values          = make_zefop(RT.Values)
keys            = make_zefop(RT.Keys)
reverse         = make_zefop(RT.Reverse)
reduce          = make_zefop(RT.Reduce)
iterate         = make_zefop(RT.Iterate)
scan            = make_zefop(RT.Scan)
group_by        = make_zefop(RT.GroupBy)
transpose       = make_zefop(RT.Transpose)
frequencies     = make_zefop(RT.Frequencies)
max             = make_zefop(RT.Max)
min             = make_zefop(RT.Min)
max_by          = make_zefop(RT.MaxBy)
min_by          = make_zefop(RT.MinBy)
first           = make_zefop(RT.First)
second          = make_zefop(RT.Second)
last            = make_zefop(RT.Last)
single          = make_zefop(RT.Single)           
single_or       = make_zefop(RT.SingleOr)
only            = make_zefop(RT.Single)                 # TODO: retire, since we renamed this to 'single'
identity        = make_zefop(RT.Identity)
take            = make_zefop(RT.Take)
take_while      = make_zefop(RT.TakeWhile)
take_while_pair = make_zefop(RT.TakeWhilePair)
take_until      = make_zefop(RT.TakeUntil)                 # TODO: use 'take_until' in the RX-sense: complete the stream based on another stream emitting an item. Add 'including' to take_while as a flag for the current behavior?
skip_while      = make_zefop(RT.SkipWhile)
drop            = make_zefop(RT.Drop)                      # TODO: retire this and switch to skip
skip            = make_zefop(RT.Skip)
length          = make_zefop(RT.Length) 
nth             = make_zefop(RT.Nth) 
now             = make_zefop(RT.Now) 
affected        = make_zefop(RT.Affected) 
merged          = make_zefop(RT.Merged)                    # TODO
terminated      = make_zefop(RT.Terminated) 
instantiated    = make_zefop(RT.Instantiated) 
value_assigned  = make_zefop(RT.ValueAssigned) 
events          = make_zefop(RT.Events) 
to_delegate     = make_zefop(RT.ToDelegate) 
delegate_of     = make_zefop(RT.DelegateOf) 
target          = make_zefop(RT.Target) 
source          = make_zefop(RT.Source) 
L               = make_zefop(RT.L)
O               = make_zefop(RT.O)
Z               = make_zefop(RT.Z)
RAE             = make_zefop(RT.RAE)
time            = make_zefop(RT.Time) 
value           = make_zefop(RT.Value)
sort            = make_zefop(RT.Sort)
instances       = make_zefop(RT.Instances)
uid             = make_zefop(RT.Uid)
frame           = make_zefop(RT.Frame)
discard_frame   = make_zefop(RT.DiscardFrame)
to_frame        = make_zefop(RT.InFrame)                           # TODO: retire this. Use 'in_frame' instead
in_frame        = make_zefop(RT.InFrame)
to_graph_slice  = make_zefop(RT.ToGraphSlice)
to_tx           = make_zefop(RT.ToTx)
time_travel     = make_zefop(RT.TimeTravel)
next_tx         = make_zefop(RT.NextTX)             
previous_tx     = make_zefop(RT.PreviousTX)     
to_ezefref      = make_zefop(RT.ToEZefRef)
root            = make_zefop(RT.Root)
terminate       = make_zefop(RT.Terminate) 
assign_value    = make_zefop(RT.AssignValue) 
is_a            = make_zefop(RT.IsA)
is_represented_as = make_zefop(RT.IsRepresentedAs)
representation_type = make_zefop(RT.RepresentationType)
rae_type        = make_zefop(RT.RaeType)
abstract_type   = make_zefop(RT.AbstractType)
fill_or_attach  = make_zefop(RT.FillOrAttach)
Assert          = make_zefop(RT.Assert)
allow_tombstone = make_zefop(RT.AllowTombstone)
tag             = make_zefop(RT.Tag)
untag           = make_zefop(RT.Untag)
sync            = make_zefop(RT.Sync)
to_clipboard    = make_zefop(RT.ToClipboard)
from_clipboard  = make_zefop(RT.FromClipboard)
text_art        = make_zefop(RT.TextArt)

sign            = make_zefop(RT.Sign)
if_then_else    = make_zefop(RT.IfThenElse)
if_then_else_apply    = make_zefop(RT.IfThenElseApply)
attempt         = make_zefop(RT.Attempt)
bypass          = make_zefop(RT.Bypass)
pattern         = make_zefop(RT.Pattern)
replace         = make_zefop(RT.Replace)
distinct        = make_zefop(RT.Distinct)
distinct_by     = make_zefop(RT.DistinctBy)
is_distinct     = make_zefop(RT.IsDistinct)
is_distinct_by  = make_zefop(RT.IsDistinctBy)
shuffle         = make_zefop(RT.Shuffle)
split           = make_zefop(RT.Split)
split_if        = make_zefop(RT.SplitIf)
graphviz        = make_zefop(RT.Graphviz)

tx              = make_zefop(RT.Tx)
exists_at       = make_zefop(RT.ExistsAt)
base_uid        = make_zefop(RT.BaseUid)
origin_uid      = make_zefop(RT.OriginUid)
origin_rae      = make_zefop(RT.OriginRAE)

has_out         = make_zefop(RT.HasOut)                # z1 | has_out[RT.Foo] use  (z1, RT.Foo, Z) | exists  /   (z, RT.Foo, RAE) | exists[g]  /   (z, RT.Foo, RAE) | exists[now(g)][single]
has_in          = make_zefop(RT.HasIn)                 # z1 | has_in[RT.Foo]  use  (Z, RT.Foo, z1) | exists

All             = make_zefop(RT.All)               # TODO: retire

In              = make_zefop(RT.In)
Ins             = make_zefop(RT.Ins)
Out             = make_zefop(RT.Out)
Outs            = make_zefop(RT.Outs)
in_rel          = make_zefop(RT.InRel)
in_rels         = make_zefop(RT.InRels)
out_rel         = make_zefop(RT.OutRel)
out_rels        = make_zefop(RT.OutRels)


is_zefref_promotable= make_zefop(RT.IsZefRefPromotable)  # Retire this. this is a old love level operator. We can use is_a[RAE] or an extended concept new.
ins             = make_zefop(RT.InsOld)                   # z < L[RT]
outs            = make_zefop(RT.OutsOld)                  # z > L[RT]
ins_and_outs    = make_zefop(RT.InsAndOutsOld)            # we don't seem to use this very much (looking at existing code base).
time_slice      = make_zefop(RT.TimeSlice)             # with the GraphSlice syntax it seems that we have been using time slices / txs mostly as a proxy for that. Given the data oriented approach, it seems a more consistent design to just use Int instead of time slices when we actually want a number?
    
instantiation_tx= make_zefop(RT.InstantiationTx)       # use tx[instantiated]
termination_tx  = make_zefop(RT.TerminationTx)         # use tx[terminated]   
relations       = make_zefop(RT.Relations)             # g | now | All[(z1, RT.Bar, z2)]   with pattern matching style any of the three args can also be replaced with a more general class
relation        = make_zefop(RT.Relation)              # looking through our code base for use cases of this op, I don't think a separate operator is necessary. Just use the syntax above followed by ... | single. If required more often, it is much easier to add this in future than to remove it.
call            = make_zefop(RT.Call)                  # useful now. But the "func[...]" syntax could replace this in future?
unpack = make_zefop(RT.Unpack)
_any            = make_zefop(RT._Any)                  # used as a wildcard
has_relation    = make_zefop(RT.HasRelation)     

replace_at      = make_zefop(RT.ReplaceAt)           
pad_to_length   = make_zefop(RT.PadToLength)           
pad_left        = make_zefop(RT.PadLeft)           
pad_right       = make_zefop(RT.PadRight)           
pad_center      = make_zefop(RT.PadCenter)           
ceil            = make_zefop(RT.Ceil)           
floor           = make_zefop(RT.Floor)           
round           = make_zefop(RT.Round)           
random_pick     = make_zefop(RT.RandomPick)           

to_json         = make_zefop(RT.ToJSON)           
from_json       = make_zefop(RT.FromJSON)   

to_yaml         = make_zefop(RT.ToYaml)
from_yaml       = make_zefop(RT.FromYaml)

to_toml         = make_zefop(RT.ToToml)
from_toml       = make_zefop(RT.FromToml)

to_csv         = make_zefop(RT.ToCSV)
from_csv       = make_zefop(RT.FromCSV)

read_file      = make_zefop(RT.ReadFile)
load_file      = make_zefop(RT.LoadFile)
write_file     = make_zefop(RT.WriteFile)
save_file      = make_zefop(RT.SaveFile)


pandas_to_gd = make_zefop(RT.PandasToGd)

as_pipeline    = make_zefop(RT.AsPipeline)
inject         = make_zefop(RT.Inject)
inject_list    = make_zefop(RT.InjectList)


is_alpha        = make_zefop(RT.IsAlpha)
is_numeric      = make_zefop(RT.IsNumeric)
is_alpha_numeric= make_zefop(RT.IsAlphaNumeric)
to_upper_case   = make_zefop(RT.ToUpperCase)
to_lower_case   = make_zefop(RT.ToLowerCase)

to_pascal_case  = make_zefop(RT.ToPascalCase)
to_camel_case   = make_zefop(RT.ToCamelCase)
to_kebab_case   = make_zefop(RT.ToKebabCase)
to_snake_case   = make_zefop(RT.ToSnakeCase)
to_screaming_snake_case = make_zefop(RT.ToScreamingSnakeCase)


make_request  = make_zefop(RT.MakeRequest)


zascii_to_asg    = make_zefop(RT.ZasciiToAsg)
zascii_to_schema = make_zefop(RT.ZasciiToSchema)

                # Syntax????????????????? 
                # has_relation(z1, RT.Foo, z2)      replaced by 
                #       1) (z1, RT.Foo, z2) | exists[g_slice]   or 
                #       1) (z1, RT.Foo, z2) | contained_in[g_slice]   or 
                #       2) g_slice | contains[(z1, RT.Foo, z2)]
# Syntax choices:   
#       exists or contained_in?
#       All or instances?    Also: my_delegate | All    or my_delegate | instances?





merge           = make_zefop(RT.Merge)                 # We need this for observables. Only there?

blake3          = make_zefop(RT.Blake3)
to_zef_list     = make_zefop(RT.ToZefList)
transact        = make_zefop(RT.Transact)
# transact

# subscribe
# keep_alive
# incoming
# outgoing
# on_instantiation
# on_termination
# on_value_assignment








# TODO: implement
on              = make_zefop(RT.On)         


peel            = make_zefop(RT.Peel)                
match           = make_zefop(RT.Match)                
match_apply     = make_zefop(RT.MatchApply,)                
Range           = make_zefop(RT.Range,)                
# match
# split_before
# split_after
# split_at
# split_when



# delay
# window(Max[10], Max[30/sec, over[2*sec]])
# 
# time_travel         # RuntimeError: Only(EZefRefs zs) request, but length was 0
