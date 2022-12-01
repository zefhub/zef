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

from .. import report_import
report_import("zef.core._ops")

from .VT import RT
from .op_structs import  evaluating, LazyValue, Awaitable, ZefOp, CollectingOp, SubscribingOp, ForEachingOp
from . import internals

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

run             = ZefOp(((internals.RT.Run, (evaluating,)), ))

def make_zefop(rt):
    return ZefOp(((rt, ()), ))

cycle           = make_zefop(internals.RT.Cycle)
repeat          = make_zefop(internals.RT.Repeat)
contains        = make_zefop(internals.RT.Contains)
contained_in    = make_zefop(internals.RT.ContainedIn)
skip            = make_zefop(internals.RT.Skip)
all             = make_zefop(internals.RT.All)
any             = make_zefop(internals.RT.Any)
slice           = make_zefop(internals.RT.Slice)                 # lowercase slice is used in core Python
join            = make_zefop(internals.RT.Join)
trim            = make_zefop(internals.RT.Trim)
trim_left       = make_zefop(internals.RT.TrimLeft)
trim_right      = make_zefop(internals.RT.TrimRight)
yo              = make_zefop(internals.RT.Yo)
tap             = make_zefop(internals.RT.Tap)
push            = make_zefop(internals.RT.Push)

cartesian_product = make_zefop(internals.RT.CartesianProduct)
permutations    = make_zefop(internals.RT.Permutations)
combinations    = make_zefop(internals.RT.Combinations)
sum             = make_zefop(internals.RT.Sum)
product         = make_zefop(internals.RT.Product)
add             = make_zefop(internals.RT.Add)
subtract        = make_zefop(internals.RT.Subtract)
multiply        = make_zefop(internals.RT.Multiply)
divide          = make_zefop(internals.RT.Divide)
mean            = make_zefop(internals.RT.Mean)
variance        = make_zefop(internals.RT.Variance)
power           = make_zefop(internals.RT.Power)
exponential     = make_zefop(internals.RT.Exponential)
logarithm       = make_zefop(internals.RT.Logarithm)

equals          = make_zefop(internals.RT.Equals)
greater_than    = make_zefop(internals.RT.GreaterThan)
less_than       = make_zefop(internals.RT.LessThan)
greater_than_or_equal = make_zefop(internals.RT.GreaterThanOrEqual)
less_than_or_equal = make_zefop(internals.RT.LessThanOrEqual)
Not             = make_zefop(internals.RT.Not)
And             = make_zefop(internals.RT.And)
Or              = make_zefop(internals.RT.Or)
xor             = make_zefop(internals.RT.Xor)

always          = make_zefop(internals.RT.Always)
docstring       = make_zefop(internals.RT.Docstring)
source_code     = make_zefop(internals.RT.SourceCode)

absorbed        = make_zefop(internals.RT.Absorbed)
without_absorbed= make_zefop(internals.RT.WithoutAbsorbed)

get_in          = make_zefop(internals.RT.GetIn)
insert_in       = make_zefop(internals.RT.InsertIn)
remove_in       = make_zefop(internals.RT.RemoveIn)
update_in       = make_zefop(internals.RT.UpdateIn)
update_at       = make_zefop(internals.RT.UpdateAt)
insert_at       = make_zefop(internals.RT.InsertAt)
update          = make_zefop(internals.RT.Update)
remove_at       = make_zefop(internals.RT.RemoveAt)
merge           = make_zefop(internals.RT.Merge)
merge_with      = make_zefop(internals.RT.MergeWith)
int_to_alpha    = make_zefop(internals.RT.IntToAlpha)
permute_to      = make_zefop(internals.RT.PermuteTo)


# Implemented Lazy ZefOps
expect          = make_zefop(internals.RT.Expect)
filter          = make_zefop(internals.RT.Filter)
without         = make_zefop(internals.RT.Without)
select_keys     = make_zefop(internals.RT.SelectKeys)
modulo          = make_zefop(internals.RT.Modulo)
select_by_field = make_zefop(internals.RT.SelectByField)
apply_functions = make_zefop(internals.RT.ApplyFunctions)
map             = make_zefop(internals.RT.Map)
map_cat         = make_zefop(internals.RT.MapCat)
identity        = make_zefop(internals.RT.Identity)
concat          = make_zefop(internals.RT.Concat)
zip             = make_zefop(internals.RT.Zip)
prepend         = make_zefop(internals.RT.Prepend)
append          = make_zefop(internals.RT.Append)
interleave      = make_zefop(internals.RT.Interleave)
interleave_longest = make_zefop(internals.RT.InterleaveLongest)
chunk           = make_zefop(internals.RT.Chunk)
sliding         = make_zefop(internals.RT.Sliding)
stride          = make_zefop(internals.RT.Stride)
insert          = make_zefop(internals.RT.Insert)
insert_into     = make_zefop(internals.RT.InsertInto)
reverse_args    = make_zefop(internals.RT.ReverseArgs)
remove          = make_zefop(internals.RT.Remove)
get             = make_zefop(internals.RT.Get)
get_field       = make_zefop(internals.RT.GetField)
enumerate       = make_zefop(internals.RT.Enumerate)
items           = make_zefop(internals.RT.Items)
values          = make_zefop(internals.RT.Values)
keys            = make_zefop(internals.RT.Keys)
reverse         = make_zefop(internals.RT.Reverse)
reduce          = make_zefop(internals.RT.Reduce)
iterate         = make_zefop(internals.RT.Iterate)
scan            = make_zefop(internals.RT.Scan)
group_by        = make_zefop(internals.RT.GroupBy)
group           = make_zefop(internals.RT.Group)
transpose       = make_zefop(internals.RT.Transpose)
frequencies     = make_zefop(internals.RT.Frequencies)
max             = make_zefop(internals.RT.Max)
min             = make_zefop(internals.RT.Min)
max_by          = make_zefop(internals.RT.MaxBy)
min_by          = make_zefop(internals.RT.MinBy)
clamp           = make_zefop(internals.RT.Clamp)
first           = make_zefop(internals.RT.First)
second          = make_zefop(internals.RT.Second)
last            = make_zefop(internals.RT.Last)
single          = make_zefop(internals.RT.Single)           
single_or       = make_zefop(internals.RT.SingleOr)
only            = make_zefop(internals.RT.Single)                 # TODO: retire, since we renamed this to 'single'
identity        = make_zefop(internals.RT.Identity)
take            = make_zefop(internals.RT.Take)
take_while      = make_zefop(internals.RT.TakeWhile)
take_while_pair = make_zefop(internals.RT.TakeWhilePair)
take_until      = make_zefop(internals.RT.TakeUntil)                 # TODO: use 'take_until' in the RX-sense: complete the stream based on another stream emitting an item. Add 'including' to take_while as a flag for the current behavior?
skip_while      = make_zefop(internals.RT.SkipWhile)
skip_until      = make_zefop(internals.RT.SkipUntil)
skip            = make_zefop(internals.RT.Skip)
length          = make_zefop(internals.RT.Length) 
nth             = make_zefop(internals.RT.Nth) 
now             = make_zefop(internals.RT.Now) 
events          = make_zefop(internals.RT.Events) 
preceding_events= make_zefop(internals.RT.PrecedingEvents) 
to_delegate     = make_zefop(internals.RT.ToDelegate) 
delegate_of     = make_zefop(internals.RT.DelegateOf) 
target          = make_zefop(internals.RT.Target) 
source          = make_zefop(internals.RT.Source) 
L               = make_zefop(internals.RT.L)
O               = make_zefop(internals.RT.O)
Z               = make_zefop(internals.RT.Z)
# RAE             = make_zefop(internals.RT.RAE)
time            = make_zefop(internals.RT.Time) 
value           = make_zefop(internals.RT.Value)
sort            = make_zefop(internals.RT.Sort)
uid             = make_zefop(internals.RT.Uid)
frame           = make_zefop(internals.RT.Frame)
discard_frame   = make_zefop(internals.RT.DiscardFrame)
to_frame        = make_zefop(internals.RT.InFrame)                           # TODO: retire this. Use 'in_frame' instead
in_frame        = make_zefop(internals.RT.InFrame)
to_graph_slice  = make_zefop(internals.RT.ToGraphSlice)
to_tx           = make_zefop(internals.RT.ToTx)
time_travel     = make_zefop(internals.RT.TimeTravel)
next_tx         = make_zefop(internals.RT.NextTX)             
previous_tx     = make_zefop(internals.RT.PreviousTX)     
to_ezefref      = make_zefop(internals.RT.ToEZefRef)
root            = make_zefop(internals.RT.Root)
terminate       = make_zefop(internals.RT.Terminate) 
assign          = make_zefop(internals.RT.Assign) 
is_a            = make_zefop(internals.RT.IsA)
is_represented_as = make_zefop(internals.RT.IsRepresentedAs)
representation_type = make_zefop(internals.RT.RepresentationType)
rae_type        = make_zefop(internals.RT.RaeType)
abstract_type   = make_zefop(internals.RT.AbstractType)
fill_or_attach  = make_zefop(internals.RT.FillOrAttach)
set_field       = make_zefop(internals.RT.SetField)
Assert          = make_zefop(internals.RT.Assert)
allow_tombstone = make_zefop(internals.RT.AllowTombstone)
tag             = make_zefop(internals.RT.Tag)
untag           = make_zefop(internals.RT.Untag)
sync            = make_zefop(internals.RT.Sync)
to_clipboard    = make_zefop(internals.RT.ToClipboard)
from_clipboard  = make_zefop(internals.RT.FromClipboard)
text_art        = make_zefop(internals.RT.TextArt)

sign            = make_zefop(internals.RT.Sign)
attempt         = make_zefop(internals.RT.Attempt)
bypass          = make_zefop(internals.RT.Bypass)
pattern         = make_zefop(internals.RT.Pattern)
replace         = make_zefop(internals.RT.Replace)
distinct        = make_zefop(internals.RT.Distinct)
distinct_by     = make_zefop(internals.RT.DistinctBy)
is_distinct     = make_zefop(internals.RT.IsDistinct)
is_distinct_by  = make_zefop(internals.RT.IsDistinctBy)
shuffle         = make_zefop(internals.RT.Shuffle)
split           = make_zefop(internals.RT.Split)
split_left      = make_zefop(internals.RT.SplitLeft)
split_right     = make_zefop(internals.RT.SplitRight)
graphviz        = make_zefop(internals.RT.Graphviz)

blueprint       = make_zefop(internals.RT.Blueprint)
exists_at       = make_zefop(internals.RT.ExistsAt)
aware_of        = make_zefop(internals.RT.AwareOf)
base_uid        = make_zefop(internals.RT.BaseUid)
origin_uid      = make_zefop(internals.RT.OriginUid)

has_out         = make_zefop(internals.RT.HasOut)                # z1 | has_out[RT.Foo] use  (z1, RT.Foo, Z) | exists  /   (z, RT.Foo, RAE) | exists[g]  /   (z, RT.Foo, RAE) | exists[now(g)][single]
has_in          = make_zefop(internals.RT.HasIn)                 # z1 | has_in[RT.Foo]  use  (Z, RT.Foo, z1) | exists

In              = make_zefop(internals.RT.In)
Ins             = make_zefop(internals.RT.Ins)
Out             = make_zefop(internals.RT.Out)
Outs            = make_zefop(internals.RT.Outs)
ins_and_outs    = make_zefop(internals.RT.InsAndOuts)
in_rel          = make_zefop(internals.RT.InRel)
in_rels         = make_zefop(internals.RT.InRels)
out_rel         = make_zefop(internals.RT.OutRel)
out_rels        = make_zefop(internals.RT.OutRels)


is_zefref_promotable= make_zefop(internals.RT.IsZefRefPromotable)  # Retire this. this is a old love level operator. We can use is_a[RAE] or an extended concept new.
time_slice      = make_zefop(internals.RT.TimeSlice)        
graph_slice_index=make_zefop(internals.RT.GraphSliceIndex) 
    
instantiation_tx= make_zefop(internals.RT.InstantiationTx)       # use tx[instantiated]
termination_tx  = make_zefop(internals.RT.TerminationTx)         # use tx[terminated]   
relations       = make_zefop(internals.RT.Relations)             # g | now | all[(z1, RT.Bar, z2)]   with pattern matching style any of the three args can also be replaced with a more general class
relation        = make_zefop(internals.RT.Relation)              # looking through our code base for use cases of this op, I don't think a separate operator is necessary. Just use the syntax above followed by ... | single. If required more often, it is much easier to add this in future than to remove it.
unpack          = make_zefop(internals.RT.Unpack)
# _any            = make_zefop(internals.RT._Any)                  # used as a wildcard
has_relation    = make_zefop(internals.RT.HasRelation)     

replace_at      = make_zefop(internals.RT.ReplaceAt)           
pad_left        = make_zefop(internals.RT.PadLeft)           
pad_right       = make_zefop(internals.RT.PadRight)           
pad_center      = make_zefop(internals.RT.PadCenter)           
ceil            = make_zefop(internals.RT.Ceil)           
floor           = make_zefop(internals.RT.Floor)           
round           = make_zefop(internals.RT.Round)           
random_pick     = make_zefop(internals.RT.RandomPick)           

to_json         = make_zefop(internals.RT.ToJSON)           
from_json       = make_zefop(internals.RT.FromJSON)   

to_yaml         = make_zefop(internals.RT.ToYaml)
from_yaml       = make_zefop(internals.RT.FromYaml)

to_toml         = make_zefop(internals.RT.ToToml)
from_toml       = make_zefop(internals.RT.FromToml)

to_csv         = make_zefop(internals.RT.ToCSV)
from_csv       = make_zefop(internals.RT.FromCSV)

read_file      = make_zefop(internals.RT.ReadFile)
load_file      = make_zefop(internals.RT.LoadFile)
write_file     = make_zefop(internals.RT.WriteFile)
save_file      = make_zefop(internals.RT.SaveFile)


pandas_to_gd = make_zefop(internals.RT.PandasToGd)

to_pipeline    = make_zefop(internals.RT.ToPipeline)
inject         = make_zefop(internals.RT.Inject)
inject_list    = make_zefop(internals.RT.InjectList)


is_alpha        = make_zefop(internals.RT.IsAlpha)
is_numeric      = make_zefop(internals.RT.IsNumeric)
is_alpha_numeric= make_zefop(internals.RT.IsAlphaNumeric)
to_upper_case   = make_zefop(internals.RT.ToUpperCase)
to_lower_case   = make_zefop(internals.RT.ToLowerCase)

to_pascal_case  = make_zefop(internals.RT.ToPascalCase)
to_camel_case   = make_zefop(internals.RT.ToCamelCase)
to_kebab_case   = make_zefop(internals.RT.ToKebabCase)
to_snake_case   = make_zefop(internals.RT.ToSnakeCase)
to_screaming_snake_case = make_zefop(internals.RT.ToScreamingSnakeCase)


make_request  = make_zefop(internals.RT.MakeRequest)


zascii_to_asg       = make_zefop(internals.RT.ZasciiToAsg)
zascii_to_flatgraph = make_zefop(internals.RT.ZasciiToFlatGraph)
zascii_to_blueprint_fg = make_zefop(internals.RT.ZasciiToBlueprintFg)

                # Syntax????????????????? 
                # has_relation(z1, RT.Foo, z2)      replaced by 
                #       1) (z1, RT.Foo, z2) | exists[g_slice]   or 
                #       1) (z1, RT.Foo, z2) | contained_in[g_slice]   or 
                #       2) g_slice | contains[(z1, RT.Foo, z2)]
# Syntax choices:   
#       exists or contained_in?
#       All or instances?    Also: my_delegate | all    or my_delegate | instances?





merge           = make_zefop(internals.RT.Merge)                 # We need this for observables. Only there?

blake3          = make_zefop(internals.RT.Blake3)
value_hash      = make_zefop(internals.RT.ValueHash)        
to_zef_list     = make_zefop(internals.RT.ToZefList)
transact        = make_zefop(internals.RT.Transact)
# transact

# subscribe
# keep_alive
# incoming
# outgoing
# on_instantiation
# on_termination
# on_value_assignment


peel            = make_zefop(internals.RT.Peel)                
match           = make_zefop(internals.RT.Match)                
match_on        = make_zefop(internals.RT.MatchOn)                
Range           = make_zefop(internals.RT.Range)      
zstandard_compress = make_zefop(internals.RT.ZstandardCompress)
zstandard_decompress = make_zefop(internals.RT.ZstandardDecompress)
to_bytes        = make_zefop(internals.RT.ToBytes)
utf8bytes_to_string  = make_zefop(internals.RT.Utf8bytesToString)
base64string_to_bytes = make_zefop(internals.RT.Base64stringToBytes)
bytes_to_base64string = make_zefop(internals.RT.BytesToBase64string)
is_between      = make_zefop(internals.RT.IsBetween)
If              = make_zefop(internals.RT.If)

field           = make_zefop(internals.RT.Field)
fields          = make_zefop(internals.RT.Fields)
apply           = make_zefop(internals.RT.Apply)
split_on_next   = make_zefop(internals.RT.SplitOnNext)
indexes_of      = make_zefop(internals.RT.IndexesOf)
to_flatgraph    = make_zefop(internals.RT.ToFlatGraph)
parse           = make_zefop(internals.RT.Parse)


examples        = make_zefop(internals.RT.Examples)
signature       = make_zefop(internals.RT.Signature)
tags            = make_zefop(internals.RT.Tags)
related_ops     = make_zefop(internals.RT.RelatedOps)
operates_on     = make_zefop(internals.RT.OperatesOn)
used_for        = make_zefop(internals.RT.UsedFor)


# TODO: implement
on              = make_zefop(internals.RT.On)         
gather          = make_zefop(internals.RT.Gather)
alias           = make_zefop(internals.RT.Alias)
splice          = make_zefop(internals.RT.Splice)
flatten_dict    = make_zefop(internals.RT.FlattenDict)
unflatten_dict  = make_zefop(internals.RT.UnflattenDict)


token_name      = make_zefop(internals.RT.TokenName)


zef_id      = make_zefop(internals.RT.ZefID)
          
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
