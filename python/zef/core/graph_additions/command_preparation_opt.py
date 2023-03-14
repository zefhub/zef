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
report_import("zef.core.graph_additions.command_preparation")

from .common_opt import *

from ..atom import _get_atom_type, _get_atom_id, _get_fields, _get_ref_pointer

##############################################
# * Preparation dispatch
#--------------------------------------------

def dispatch_preparation(cmd, gs, context):
    if is_a_Level2AtomClass(cmd):
        return prepare_obj_notation(cmd, gs, context)
    elif is_a_PleaseRun(cmd):
        return prepare_please_run(cmd, gs, context)
    elif is_a_PleaseInstantiate(cmd) and is_a_PleaseInstantiateRelation(cmd._value["atom"]):
        return prepare_relations(cmd, gs, context)
    elif is_a_PleaseCommandLevel1(cmd):
        return prepare_pass_through(cmd, gs, context)
    else:
        not_implemented_error(cmd, "Unknown cmd type for preparation")


# TODO: This should be possible to process even at interpretation phase if we
# know that a new object is to be created. Hence, this code should be split off,
# into a part that handles the "no object on graph" and a part that handles an
# existing object. They can use the same code for final application.
def prepare_obj_notation(cmd, gs, context):
    gen_id_state = context["gen_id_state"]
    from .wish_tagging_opt import ensure_tag

    cmds = []

    cmd, me, gen_id_state = ensure_tag(cmd, gen_id_state)

    z_on_graph = None
    need_to_create = False
    if is_a_WishID(me):
        if _get_atom_type(cmd) is None:
            pass
        elif is_a_RT(_get_atom_type(cmd)):
            need_to_create = False
        else:
            need_to_create = True
    elif is_a_EternalUID(me):
        if is_a_RT(_get_atom_type(cmd)):
            need_to_create = False
        else:
            z_on_graph = find_rae_in_target(me, gs)
            if z_on_graph is None:
                need_to_create = True
    else:
        raise Exception(f"Object notation got a weird value for its own ID: {me}")

    if need_to_create:
        # TODO: Because the "pure ET with a name" should actually be an atom
        # (the ET.x[y] is a legacy style), this might have to change into
        # something more directly the pure ET creation, i.e. a PleaseInstantiate
        # itself directly.
        if is_a_ET(_get_atom_type(cmd)) or is_a_AET(_get_atom_type(cmd)):
            # TODO: Probably need to handle multiple names here at some point
            # assert len(get_names(cmd)) == 1
            # cmds += [rae_type(cmd)[me]]
            if is_a_EternalUID(me):
                cmds += [UVT_ctor_opt(PleaseInstantiate, dict(
                    atom=_get_atom_type(cmd),
                    origin_uid=me,
                ))]
            else:
                cmds += [UVT_ctor_opt(PleaseInstantiate, dict(
                    atom=_get_atom_type(cmd),
                    internal_ids=(me,),
                ))]
        elif is_a_RT(_get_atom_type(cmd)):
            raise Exception("Shouldn't get here anymore")
        else:
            raise NotImplementedError(f"TODO can't create something without a known type: {cmd}")

    obj_id = force_as_id(me)

    from ..atom import _get_fields
    fields = _get_fields(cmd)
    more_cmds,gen_id_state = nested_prepare_obj_fields(obj_id, z_on_graph, fields, gen_id_state)
    cmds += more_cmds

    context = dict(context)
    context["gen_id_state"] = gen_id_state
    return [], cmds, context

def nested_prepare_obj_fields(obj_id, z_on_graph, fields, gen_id_state):
    from .wish_tagging_opt import ensure_tag, Taggable
    cmds = []

    for k, (obj, v) in fields.items():
        rt = RT(k)
        # Going to assume we have the correct types getting here
        if type(v) != set:
            v = {v}

        if type(v) == set:
            # First go through and figure out the minimal set of changes.
            #
            # to_create = make a new relation and possibly a new AE (if it is a value)
            # to_terminate = termiante an old relation
            # to_assign = reuse an existing relation + AE and assign a new value
            # to_keep = leave this relation alone
            if z_on_graph is None:
                to_create = v
                to_terminate = []
                to_assign = []
                to_keep = []
            else:
                to_create = []
                to_assign = []
                to_keep = []

                existing_rels = pyzo.traverse_out_rels_multi(z_on_graph, get_c_token(rt))
                existing_items = {}
                for rel in existing_rels:
                    t = pyzo.target(rel)
                    if pyzo.rae_type(t) == AttributeEntityType:
                        t_val = pyzo.value(t)
                    else:
                        t_val = None
                    existing_items[rel] = (t, t_val)
                
                assign_candidates = {}
                existing_free = set(existing_rels)
                for item in v:
                    if is_aUserWishID(item):
                        to_create += [item]
                        continue
                    this_assign_candidates = []
                    for existing_rel in existing_free:
                        existing_target,existing_val = existing_items[existing_rel]
                        if type(item) == Atom_:
                            if discard_frame(item) == discard_frame(existing_target):
                                to_keep += [existing_rel]
                                existing_free.remove(existing_rel)
                                break
                        else:
                            if item == existing_val:
                                to_keep += [existing_rel]
                                existing_free.remove(existing_rel)
                                break
                            if can_assign_to(item, existing_target):
                                this_assign_candidates += [existing_rel]
                    else:
                        if type(item) == Atom_:
                            to_create += [item]
                        else:
                            assign_candidates[item] = this_assign_candidates

                for to_decide,candidates in assign_candidates.items():
                    candidates = set(candidates) & existing_free 
                    if len(candidates) == 0:
                        to_create += [to_decide]
                    else:
                        candidate = list(candidates)[0]
                        to_assign += [(to_decide, candidate)]
                        existing_free.remove(candidate)

                to_terminate = list(existing_free)
                        


            # Get an id from the specification if we have it. This id will only
            # make sense if there is exactly one item being given it (i.e. no
            # multiple relations created), and we will let the fallout after
            # this function handle that error case.
            prior_name = None
            if type(obj) == Atom_:
                # Note: this could be None
                prior_name = get_most_authorative_id_opt(obj)
            elif is_a_RT(obj):
                from ..VT.rae_types import RAET_get_names
                names = RAET_get_names(obj)
                if len(names) > 2:
                    raise Exception(f"Don't know what to do with multiple names in a RT: {names}")
                if len(names) == 1:
                    prior_name = force_as_id(names[0])

            exact_ids = []
            exact_zs = []

            for rel in to_keep:
                exact_ids += [pyzo.origin_uid(rel)]
                exact_zs += [rel]
                if prior_name is not None:
                    cmds += [UVT_ctor_opt(PleaseAlias, dict(ids=[pyzo.origin_uid(rel), prior_name]))]

            for rel in to_terminate:
                cmds += [UVT_ctor_opt(PleaseTerminate, dict(target=pyzo.origin_uid(rel)))]

            for val,rel in to_assign:
                exact_ids += [pyzo.origin_uid(rel)]
                exact_zs += [rel]
                cmds += [UVT_ctor_opt(PleaseAssign, dict(target=pyzo.origin_uid(pyzo.target(rel)), value=Val(val)))]
                if prior_name is not None:
                    cmds += [UVT_ctor_opt(PleaseAlias, dict(ids=[pyzo.origin_uid(rel), prior_name]))]

            for item in to_create:
                item,item_id,gen_id_state = ensure_tag(item, gen_id_state)
                context = {"gen_id_state": gen_id_state}
                item_ready_cmds, item_cmds, context = prepare_reduced_interpret(item, None, context)
                assert len(item_ready_cmds) == 0
                gen_id_state = context["gen_id_state"]
                cmds += item_cmds
                id_rel,gen_id_state = gen_internal_id(gen_id_state)
                cmds += [UVT_ctor_opt(PleaseInstantiate, dict(
                    atom=dict(rt=rt, source=obj_id, target=item_id),
                    internal_ids=[id_rel]
                ))]
                exact_ids += [id_rel]
                exact_zs += [None]

                if prior_name is not None:
                    # Note that we use alias here instead of including it into
                    # the PleaseInstantiate, as all names can be aliased but the
                    # instantiate requires choosing between origin_uid and
                    # internal_ids.
                    cmds += [UVT_ctor_opt(PleaseAlias, dict(ids=[id_rel, prior_name]))]

            cmds += [UVT_ctor_opt(PleaseBeSource, dict(target=obj_id, rel_ids=exact_ids, exact=True, rt=rt))]

            # Now check if we have additional information that should be added to the relations
            if type(obj) == Atom_:
                from ..atom import _get_fields
                fields = _get_fields(obj)
                if len(fields) > 0:
                    for (id,z) in zip(exact_ids, exact_zs):
                        more_cmds,gen_id_state = nested_prepare_obj_fields(id, z, fields, gen_id_state)
                        cmds += more_cmds

        # elif type(v) in {list, tuple}:
        #     list_id = gen_id()
        #     cmds.append( (me, RT(to_pascal_case(k)), ET.ZEF_List[list_id]) )

        #     # generate ids for each relation, that we can inter-connect them
        #     list_ids = [gen_id() for _ in range(len(v))]
        #     cmds.extend(list_ids 
        #             | sliding[2] 
        #             | map[lambda p: (Z[p[0]], RT.ZEF_NextElement, Z[p[1]])]
        #             | collect
        #         )
        #     for el, edge_id in zip(v, list_ids):
        #         sub_obj_instrs = expand_helper(el, gen_id)
        #         cmds.append( (ET.ZEF_List[list_id], RT.ZEF_ListElement[edge_id], sub_obj_instrs[0]) )
        #         cmds.extend(sub_obj_instrs[1:])
        # else:
        #     cmds.append( (me, RT(to_pascal_case(k)), v) )
        else:
            raise NotImplementedError(f"TODO: obj notation for {v}")

    return cmds, gen_id_state

def prepare_please_run(cmd, gs, context):
    todo = []
    cmds = []
    if isinstance(cmd.action, LazyValue):
        # Execute op one after another
        cur = cmd.action.initial_val
        ops = list(cmd.action.el_ops)
        for op in ops:
            rt,abs = peel(op)[0]
            if RT[rt] == RT.OutRel:
                names = abs | filter[Variable] | collect
                op = ZefOp(((rt, abs | filter[~Variable] | collect),))
            else:
                names = []

            cur = op(cur)
            if len(names) > 0:
                cmds += [PleaseAlias(ids=[origin_uid(cur)] + names)]

        if isinstance(cur, SetFieldAction):
            todo += [PleaseRun(action=cur)]
        else:
            raise NotImplementedError(f"TODO: PleaseRun result {cur}")
    else:
        raise NotImplementedError("TODO: PleaseRun")

    return cmds, todo, context

def prepare_relations(cmd, gs, context):
    # Reltaions getting to this point are pure, but they need to indicate their
    # implicit behaviour on the source/target nodes to effectively "claim
    # territory" that other relation-creating commands (like the EVI) need to
    # cooperate with.
    out_cmds = []
    gen_id_state = context["gen_id_state"]

    names = cmd._value.get("internal_ids", [])
    if len(names) == 0:
        id,gen_id_state = gen_internal_id(gen_id_state)
        names = [id]
        new_dict = dict(cmd._value)
        new_dict["internal_ids"] = names
        cmd = UVT_ctor_opt(PleaseInstantiate, new_dict)
    id = names[0]

    src = cmd.atom["source"]
    trg = cmd.atom["target"]
    rt = cmd.atom["rt"]

    out_cmds += [cmd]
    out_cmds += [UVT_ctor_opt(PleaseBeSource, dict(target=force_as_id(src), rel_ids=[id], exact=False, rt=rt))]
    out_cmds += [UVT_ctor_opt(PleaseBeTarget, dict(target=force_as_id(trg), rel_ids=[id], exact=False, rt=rt))]

    context = dict(context)
    context["gen_id_state"] = gen_id_state
    return out_cmds, [], context

def prepare_pass_through(cmd, gs, context):
    return [cmd], [], context

def prepare_interpret(cmd, gs, context):
    # raise NotImplementedError("TODO fallback to interpret")
    from .wish_interpretation import generate_level2_commands, default_interpretation_rules
    output = generate_level2_commands([cmd], default_interpretation_rules, context)
    # All outputs are todo from the point of view of preparation vs interpration.
    return [], output["cmds"], context

def prepare_reduced_interpret(cmd, gs, context):
    if is_a_Val(cmd):
        new_cmd = UVT_ctor_opt(PleaseInstantiate, dict(atom=cmd))
        return [], [new_cmd], context
    elif is_a_PleaseAssign(cmd):
        from .wish_interpretation_opt import lvl2cmds_for_ETorAET
        assert is_a_AET(cmd._value["target"])
        temp_cmds, _, context = lvl2cmds_for_ETorAET(cmd._value["target"], context)
        aet_cmd = temp_cmds[0]
        me = aet_cmd._value["internal_ids"][0]

        new_assign_cmd = UVT_ctor_opt(PleaseAssign, dict(target=me, value=cmd._value["value"]))
        return [], [new_assign_cmd, aet_cmd], context
    elif is_a_Level2AtomClass(cmd):
        return [], [cmd], context
    elif is_a_WishID(cmd):
        return [], [], context
    else:
        raise Exception(f"Command not in reduced set: {cmd}")