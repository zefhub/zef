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

from ... import report_import
report_import("zef.core.graph_additions.wish_interpretation")

from .common import *
from .wish_tagging import Taggable, ensure_tag

##########################################
# * Level 2 generation
#----------------------------------------

Lvl2Context = Pattern[{"gen_id_state": GenIDState,
                       Optional["custom"]: Any}]

# Rules should follow the signature below, but we can't specify that just yet.
# TODO: maybe the element types should be Anys so that rules can be extended?
#InterpretationFunc = Func[GraphWishInput, Lvl2Context][List[PleaseCommandLevel2], List[GraphWishInput], Lvl2Context]
InterpretationFunc = Any


# Really want to use Subtype[GraphWishInput] instead of ValueType
def generate_level2_commands(inputs: List[GraphWishInput], rules: List[Tuple[ValueType,InterpretationFunc]]):
    # For each input
    #
    # - if it's a please command, just include
    # - if it's a simple translatable one (object notation, value, triple)
    #   - convert to sequence of commands
    # - if its a zefop, then store as PleaseRun

    output_cmds = []
    context = {
        "gen_id_state": generate_initial_state("lvl2"),
    }

    todo = list(inputs)
    while len(todo) > 0:
        input = todo.pop(0)
        cmds,new_inputs,context = (input,context) | match_rules[[
            *rules,
            (Any, not_implemented_error["Unknown type of object as input to graph wish"]),
        ]] | collect
        output_cmds += cmds
        todo = new_inputs + todo
        # print()
        # print("======")
        # print(input)
        # print(cmds)
        # print(new_inputs)
        # print()

    output = {
        "cmds": output_cmds,
    }
    if "custom" in context:
        output["custom"] = context["custom"]
    return output

@func
def lvl2cmds_for_ETorAET(input: ET | AET, context: Lvl2Context):
    names = names_of_raet(input)
    this_bare_raet = bare_raet(input)

    d = {"atom": this_bare_raet}

    ouids = set(names | filter[EternalUID] | collect)
    iids = set(names | filter[Not[EternalUID]] | collect)

    if len(ouids) >= 2:
        raise Exception(f"Can't have multiple origin uids for the one object: {ouids}")
    if len(ouids) == 1:
        d["origin_uid"] = single(ouids)

    if len(iids) > 0:
        d["internal_ids"] = tuple(iids)

    return [PleaseInstantiate(d)], [], context

@func
def lvl2cmds_for_delegate(input: Delegate, context: Lvl2Context):
    d = to_delegate(input)
    cmd = PleaseInstantiate({"atom": d})

    return [cmd], [], context

@func
def lvl2cmds_for_valuenode(input: WrappedValue, context: Lvl2Context):
    cmd = PleaseInstantiate({"atom": input})

    return [cmd], [], context

@func
def lvl2cmds_for_relation_triple(input: RelationTriple, context: Lvl2Context):
    names = names_of_raet(input[1])
    bare_rt = bare_raet(input[1])

    gen_id_state = context["gen_id_state"]

    src_obj, src, gen_id_state = ensure_tag(input[0], gen_id_state)
    trg_obj, trg, gen_id_state = ensure_tag(input[2], gen_id_state)

    d = {
        "atom": {
            "rt": bare_rt,
            "source": src,
            "target": trg,
        }
    }
    euids = [x for x in names if isinstance(x, EternalUID)]
    others = [x for x in names if not isinstance(x, EternalUID)]
    if len(euids) > 0:
        assert len(euids) == 1
        d["origin_uid"] = euids[0]
    if len(others) > 0:
        d["internal_ids"] = others

    cmds = [PleaseInstantiate(d)]

    # If the RT is itself an atom with further fields, we need to pass those fields through too
    if isinstance(input[1], RelationAtom):
        cmds += [input[1]]

    context = context | insert["gen_id_state"][gen_id_state] | collect
    return cmds, [src_obj, trg_obj], context

@func
def OS_lvl2cmds_for_relation_triple(input: OldStyleRelationTriple, context: Lvl2Context):
    gen_id_state = context["gen_id_state"]

    more_inputs = []

    if len(input) == 2:
        s, rels = input
        src_obj, src, gen_id_state = ensure_tag(s, gen_id_state)
        more_inputs += [src_obj]

        for rt,t in rels:
            trg_obj, trg, gen_id_state = ensure_tag(t, gen_id_state)
            more_inputs += [trg_obj]
            more_inputs += [(src, rt, trg)]

    elif len(input) == 3:
        if isinstance(input[0], List):
            sources = input[0]
        else:
            sources = [input[0]]

        if isinstance(input[2], List):
            targets = input[2]
        else:
            targets = [input[2]]
        rt = input[1]

        src_names = []
        for src in sources:
            src_obj, src, gen_id_state = ensure_tag(src, gen_id_state)
            more_inputs += [src_obj]
            src_names += [src]
        trg_names = []
        for trg in targets:
            trg_obj, trg, gen_id_state = ensure_tag(trg, gen_id_state)
            more_inputs += [trg_obj]
            trg_names += [trg]

        more_inputs += [(s,rt,t) for s in src_names for t in trg_names]

    context = context | insert["gen_id_state"][gen_id_state] | collect
    return [], more_inputs, context

@func
def lvl2cmds_for_aet_with_value(input: AETWithValue, context: Lvl2Context):
    gen_id_state = context["gen_id_state"]
    input, me, gen_id_state = ensure_tag(input, gen_id_state)

    lvl2_cmds = [PleaseInstantiate({"atom": input.aet,
                                    "internal_ids": input.internal_ids}),
                 PleaseAssign({"target": me,
                               "value": input.value})]
            
    return lvl2_cmds, [], context

# @func
# Note: can't be a func, can't allow this to participate in the normal
# evaluation engine
def lvl2cmds_for_lazyvalue(input: LazyValue, context: Lvl2Context):
    from .obj_evaluation import evaluate_chain

    lvl2cmds = []
    further_cmds = []
    if isinstance(input.initial_val, AtomClass | Variable):
        obj = input.initial_val
        gen_id_state = context["gen_id_state"]
        final_state = evaluate_chain(obj, input.el_ops, gen_id_state)
        gen_id_state = final_state["gen_id_state"]
        further_cmds += final_state["emitted_cmds"]
        if isinstance(final_state["obj"], Nil | Variable):
            pass
        else:
            if final_state["obj"] == input:
                raise Exception("lvl2cmds is getting stuck in infinite loop when evaluating LazyValue")
            further_cmds += [final_state["obj"]]
    elif isinstance(input.initial_val, BlobPtr):
        obj = Atom(input.initial_val)
        new_lv = obj | input.el_ops
        further_cmds += [new_lv]
    elif isinstance(input.initial_val, FlatRef):
        obj = Atom(input.initial_val)
        new_lv = obj | input.el_ops
        further_cmds += [new_lv]
    elif isinstance(input.initial_val, RAERef):
        obj = Atom(input.initial_val)
        new_lv = obj | input.el_ops
        further_cmds += [new_lv]
    elif isinstance(input.initial_val, ET):
        obj = input.initial_val
        new_lv = obj() | input.el_ops
        further_cmds += [new_lv]
    elif isinstance(input.initial_val, ExtraUserAllowedIDs):
        obj = convert_extra_allowed_id(input.initial_val)
        new_lv = obj | input.el_ops
        further_cmds += [new_lv]
    else:
        raise Exception(f"Don't know what to do with an initial value of {input.initial_val}")

    return lvl2cmds, further_cmds, context


def lvl2cmds_for_rae(input: RAE, context: Lvl2Context):
    if isinstance(input, Entity|AttributeEntity):
        cmd = PleaseInstantiate(atom=rae_type(input),
                                origin_uid=origin_uid(input))
        return [cmd], [], context
    elif isinstance(input, Relation):
        gen_id_state = context["gen_id_state"]
        s_obj,s_name,gen_id_state = ensure_tag(source(input), gen_id_state)
        t_obj,t_name,gen_id_state = ensure_tag(target(input), gen_id_state)
        cmd = PleaseInstantiate(atom=dict(rt=rae_type(input),
                                          source=s_name,
                                          target=t_name),
                                origin_uid=origin_uid(input))
        context = context | insert["gen_id_state"][gen_id_state] | collect
        return [cmd], [s_obj,t_obj], context
    else:
        raise Exception("Shouldn't get here")

def lvl2cmds_for_flatref(input: FlatRef, context: Lvl2Context):
    return [], [Atom(input)], context

def lvl2cmds_for_flatrefs(input: FlatRefs, context: Lvl2Context):
    return [], list(input), context

def lvl2cmds_for_flatgraph(input: FlatGraph, context: Lvl2Context):
    new_cmds = list(input | all | collect)
    return [], new_cmds, context

@func
def lvl2cmds_for_se(input, context):
    # Note: this should not hit Variables as they will be handled in AllIDs before this
    assert not isinstance(input, Variable)

    from .obj_evaluation import cast_symbolic_expression_as_lazyvalue
    new_input = cast_symbolic_expression_as_lazyvalue(input)
    return [], [new_input], context
        

def discard_unnecessary_frame(x):
    # This function exists solely to handle Atoms with FlatRefs in them. The
    # problem with these is the presence of the FlatGraph is necessary to
    # uniquely identify them. Other Atoms can have that aspect removed.
    authority = get_most_authorative_id(x)
    if isinstance(authority, FlatRefUID):
        return x
    return discard_frame(x)
        
@func
def lvl2cmds_for_atom(atom, context):
    if isinstance(rae_type(atom), Nil):
        return [atom], [], context

    if isinstance(atom, Entity | TX | Root):
        return [discard_unnecessary_frame(atom)], [], context

    if isinstance(atom, Relation):
        # We need to ensure the relation source/target is also included.

        # If the relation atom is included without an id, either global or
        # local, then this is an error. If the atom were in a relation triple,
        # then we should have a local name by this point.
        authorative_id = get_most_authorative_id(atom)
        if authorative_id is None:
            raise Exception("Can't have relation Atom without source/target")

        if isinstance(authorative_id, WishID):
            # Good enough, source/target should be defined elsewhere
            return [atom], [], context

        # This means the atom should have a source/target. Let's find them
        # from the atom internals.

        z = find_concrete_pointer(atom)
        if z is None:
            raise Exception("Can't determine relation source/target because we can't load a concrete ZefRef for this Atom")

        src = Atom(source(z))
        trg = Atom(target(z))
        plain_rt = rae_type(atom)[authorative_id]

        return [discard_unnecessary_frame(atom)], [(src, plain_rt, trg)], context

    if isinstance(atom, AttributeEntity):
        # We need to ensure the value is also included, if the reference frame is accessible.
        #
        # If the frame is not given, we can assume "no value" is what is intended.

        authorative_id = get_most_authorative_id(atom)
        if authorative_id is None:
            return [atom], [], context

        if isinstance(authorative_id, WishID):
            # Good enough, value might be defined elsewhere
            return [atom], [], context

        from ..atom import _get_atom_id
        atom_id = _get_atom_id(atom)
        if "frame_uid" in atom_id:
            z = find_concrete_pointer(atom)
            if z is None:
                raise Exception("Can't determine value of AttributeEntity as frame is not loaded")
            val = value(z)
            if val is not None:
                cmds = [PleaseAssign(target=authorative_id, value=Val(val))]
            else:
                cmds = []
            return [discard_unnecessary_frame(atom)], cmds, context
        else:
            return [atom], [], context

    raise Exception(f"Shouldn't get here: {atom}")



@func
def OS_lvl2cmds_for_dict(input: OldStyleDict, context: Lvl2Context):
    # This must be of the form:
    # {ET.Something: {RT.One: 5, RT.Two: z}}
    assert isinstance(input, OldStyleDict)
    assert len(input) == 1

    gen_id_state = context["gen_id_state"]

    res = []

    source_input, rel_dict = single(input.items())

    source_input, me, gen_id_state = ensure_tag(source_input, gen_id_state)
    res += [source_input]

    for k,v in rel_dict.items():
        v,v_id,gen_id_state = ensure_tag(v, gen_id_state)
        res += [v]
        res += [(me, k, v_id)]

    context = context | insert["gen_id_state"][gen_id_state] | collect
    return [], res, context

@func
def pass_through(input, context):
    return [input], [], context

@func
def lvl2cmds_ignore(input, context):
    return [], [], context

default_interpretation_rules = [
    # Note that delegates and WrappedValues are included in IDs so we have to put this first.
    (Delegate, lvl2cmds_for_delegate),
    (WrappedValue, lvl2cmds_for_valuenode),
    # Ignore IDs as many commands are likely to create these
    (AllIDs, lvl2cmds_ignore),
    #
    (LazyValue, lvl2cmds_for_lazyvalue),
    (SymbolicExpression, lvl2cmds_for_se),
    # (ObjectNotation, pass_through),
    (AtomClass, lvl2cmds_for_atom),
    (PleaseCommandLevel1, pass_through),
    # (PleaseCommandLevel2, not_implemented_error["TODO"]),
    (PleaseCommandLevel2, pass_through),
    (PureET|PureAET, lvl2cmds_for_ETorAET),
    (RelationTriple, lvl2cmds_for_relation_triple),
    (AETWithValue, lvl2cmds_for_aet_with_value),
    (RAERef | RAEConcrete, lvl2cmds_for_rae),
    (FlatRef, lvl2cmds_for_flatref),
    (FlatRefs, lvl2cmds_for_flatrefs),
    (FlatGraph, lvl2cmds_for_flatgraph),
    # Things not in base version
    (OldStyleDict, OS_lvl2cmds_for_dict),
    (OldStyleRelationTriple, OS_lvl2cmds_for_relation_triple),
]
