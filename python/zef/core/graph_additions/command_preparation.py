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

from .common import *

##############################################
# * Preparation dispatch
#--------------------------------------------

def dispatch_preparation(cmd, gs, context):
    return (cmd,gs,context) | match_rules[[
        *preparation_rules,
        (Any, not_implemented_error["Unknown cmd type for preparation"]),
    ]] | collect


# TODO: This should be possible to process even at interpretation phase if we
# know that a new object is to be created. Hence, this code should be split off,
# into a part that handles the "no object on graph" and a part that handles an
# existing object. They can use the same code for final application.
def prepare_obj_notation(cmd, gs, context):
    gen_id_state = context["gen_id_state"]
    from .wish_tagging import ensure_tag, Taggable

    cmds = []

    cmd, me, gen_id_state = ensure_tag(cmd, gen_id_state)

    z_on_graph = None
    need_to_create = False
    if isinstance(me, EntityRef):
        raise Exception("Shouldn't get here anymore")
        # cmds += [me]
        # names = RAET_get_names(et)
        # if len(names) > 0:
        #     cmds += [PleaseAlias(ids=(force_as_id(me),) + names)]
        # if me in gs:
        #     z_on_graph = gs | get[me] | collect
    elif isinstance(me, WishID):
        if rae_type(cmd) is None:
            pass
        elif isinstance(rae_type(cmd), RT):
            need_to_create = False
        else:
            need_to_create = True
    elif isinstance(me, EternalUID):
        if isinstance(rae_type(cmd), RT):
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
        if isinstance(rae_type(cmd), PureET | PureAET):
            # TODO: Probably need to handle multiple names here at some point
            # assert len(get_names(cmd)) == 1
            cmds += [rae_type(cmd)[me]]
        elif isinstance(rae_type(cmd), PureRT):
            raise Exception("Shouldn't get here anymore")
        else:
            raise NotImplementedError(f"TODO can't create something without a known type: {cmd}")


    from ..atom import _get_fields
    for k, v in _get_fields(cmd).items():
        rels_exact = False
        rt = RT(k)
        if isinstance(v, PrimitiveValue | Taggable | UserWishID):
            v = {v}

        if isinstance(v, set):
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

                existing_rels = z_on_graph | out_rels[rt] | collect
                existing_items = {}
                for rel in existing_rels:
                    t = target(rel)
                    if isinstance(t, AttributeEntity):
                        t_val = value(t)
                    else:
                        t_val = None
                    existing_items[rel] = (t, t_val)
                
                assign_candidates = {}
                existing_free = set(existing_rels)
                for item in v:
                    if isinstance(item, UserWishID):
                        to_create += [item]
                        continue
                    this_assign_candidates = []
                    for existing_rel in existing_free:
                        existing_target,existing_val = existing_items[existing_rel]
                        if isinstance(item, AtomClass):
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
                        if isinstance(item, AtomClass):
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
                        

            exact_ids = []

            for rel in to_keep:
                exact_ids += [origin_uid(rel)]

            for rel in to_terminate:
                cmds += [PleaseTerminate(target=origin_uid(rel))]

            for val,rel in to_assign:
                exact_ids += [origin_uid(rel)]
                cmds += [PleaseAssign(target=origin_uid(target(rel)), value=Val(val))]

            for item in to_create:
                item,item_id,gen_id_state = ensure_tag(item, gen_id_state)
                cmds += [item]
                id_rel,gen_id_state = gen_internal_id(gen_id_state)
                cmds += [PleaseInstantiate(
                    atom=dict(rt=rt, source=me, target=item_id),
                    internal_ids=[id_rel]
                )]
                exact_ids += [id_rel]

            cmds += [PleaseBeSource(target=force_as_id(me), rel_ids=exact_ids, exact=True, rt=rt)]

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

    context = context | insert["gen_id_state"][gen_id_state] | collect
    return [], cmds, context

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

    names = cmd | get["internals_ids"][[]] | collect
    if len(names) == 0:
        id,gen_id_state = gen_internal_id(gen_id_state)
        names = [id]
        cmd = cmd._get_type()(cmd._value | insert["internal_ids"][names])
    id = names[0]

    out_cmds += [cmd]
    out_cmds += [PleaseBeSource(target=force_as_id(src), rel_id=id, exact=False, rt=cmd.atom["rt"])]
    out_cmds += [PleaseBeTarget(target=force_as_id(trg), rel_id=id, exact=False, rt=cmd.atom["rt"])]

    context = context | insert["gen_id_state"][gen_id_state] | collect
    return out_cmds, [], context

def prepare_pass_through(cmd, gs, context):
    return [cmd], [], context

def prepare_interpret(cmd, gs, context):
    # raise NotImplementedError("TODO fallback to interpret")
    from .wish_interpretation import generate_level2_commands, default_interpretation_rules
    cmds,todo,context = (cmd,context) | match_rules[[
        *default_interpretation_rules,
        (Any, not_implemented_error["Unknown cmd type for interpretation during preparation"]),
    ]] | collect
    # All outputs are todo from the point of view of preparation vs interpration.
    return [], cmds + todo, context

preparation_rules = [
    (AtomClass, prepare_obj_notation),
    (PleaseRun, prepare_please_run),
    (PleaseInstantiate & Is[get["atom"] | is_a[PleaseInstantiateRelation]], prepare_relations),
    (PleaseCommandLevel1, prepare_pass_through),
    # Fallback to interpretation rules here but make them pass back into this list always (i.e. redirect the lists)
    (GraphWishInput, prepare_interpret),
]
    