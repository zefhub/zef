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

import imgui
import sys
import yaml
from caseconverter import pascalcase

from ... import *
from ...ops import *

purpose_opts = ["entity", "field", "field_on", "ignore", "source", "target", "id"]
purpose_keys = {}
purpose_common_keys = ["name", "purpose", "data_type"]
purpose_keys["id"] = purpose_common_keys + ["ET"]
purpose_keys["entity"] = purpose_common_keys + ["ET", "RT"]
purpose_keys["field"] = purpose_common_keys + ["RT"]
purpose_keys["field_on"] = purpose_common_keys + ["RT", "target"]
purpose_keys["ignore"] = purpose_common_keys + []
purpose_keys["source"] = purpose_common_keys + ["ET"]
purpose_keys["target"] = purpose_common_keys + ["ET"]
purpose_keys["id"] = purpose_common_keys + ["RT"]

# TODO - allow ignoring of tables.
kind_opts = ["entity", "relation", "ignore"]
kind_common_keys = ["tag", "data_source", "cols", "kind"]
kind_keys = {}
kind_keys["entity"] = kind_common_keys + ["ID_col", "ET"]
kind_keys["relation"] = kind_common_keys + ["ID_col", "RT"]

##############################
# * Theming
#----------------------------

entity_background = (4/255, 32/255, 194/255)
relation_background = (4/255, 125/255, 91/255)

entity_foreground = (120/255, 133/255, 254/255)
special_foreground = (174/255, 79/255, 214/255)

active_step_color = (0.8, 0.1, 0.1)
done_step_color = (0.0, 0.0, 0.0)
done_step_text_color = (0.2, 0.2, 0.2)

##################################
# * Data wranglers
#--------------------------------


from dataclasses import dataclass
@dataclass
class Empty:
    pass

def maybe_dict_as_obj(v):
    if isinstance(v, dict):
        return dict_as_obj(v)
    if isinstance(v, tuple):
        return tuple([dict_as_obj(x) for x in v])
    if isinstance(v, list):
        return [dict_as_obj(x) for x in v]
    return v

def dict_as_obj(d):
    obj = Empty()
    for key,val in d.items():
        val = maybe_dict_as_obj(val)
        setattr(obj, key, val)
    return obj

def obj_as_dict(obj):
    if isinstance(obj, Empty):
        d = {}
        for key in keys(obj):
            val = getattr(obj, key)
            val = obj_as_dict(val)
            d[key] = val
        return d
    if isinstance(obj, list):
        return [obj_as_dict(x) for x in obj]
    return obj

def get(obj, key, default):
    if hasattr(obj, key):
        return getattr(obj, key)
    return default

def keys(obj):
    return [x for x in dir(obj) if x[0] != '_']

def get_col(cols, name):
    for col in cols:
        if col.name == name:
            return col

    return None

def get_col_name_from_purpose(cols, purpose):
    col = [x.name for x in cols if x.purpose == purpose]
    if col:
        return col[0]
    else:
        return None


def change_col_to_purpose(item, new_purpose):
    if new_purpose == "entity":
        item.ET = get(item, "ET", get(item, "RT", item.name))
        item.RT = get(item, "RT", get(item, "ET", item.name))
    elif new_purpose == "field":
        item.RT = get(item, "RT", get(item, "ET", item.name))
    elif new_purpose == "field_on":
        item.RT = get(item, "RT", get(item, "ET", item.name))
        item.target = get(item, "target", get(item, "ET", ""))
    elif new_purpose == "ignore":
        pass
    elif new_purpose == "source":
        item.ET = get(item, "ET", get(item, "ET", item.name))
    elif new_purpose == "target":
        item.ET = get(item, "ET", get(item, "ET", item.name))
    elif new_purpose == "id":
        item.RT = get(item, "RT", get(item, "RT", "ID"))
    else:
        raise Exception(f"Don't understand purpose: '{purpose}'")
    item.purpose = new_purpose

    if "data_type" in purpose_keys[new_purpose]:
        item.data_type = get(item, "data_type", "String")

    for key in set(keys(item)).difference(purpose_keys[new_purpose]):
        delattr(item, key)

    return item


##############################
# * Init
#----------------------------


def init_state(filepath):
    with open(filepath) as file:
        decl = yaml.safe_load(file)

    imgui.get_style().window_rounding = 0
    S = Empty()
    S.decl = dict_as_obj(decl)
    S.save_filename = filepath
    S.cur_step = 0
    return S

##############################
# * Render
#----------------------------



def render(S):
    if imgui.begin_main_menu_bar():
        clicked_save, _ = imgui.menu_item(
            "Save", "", False, True
        )
        clicked_quit, _ = imgui.menu_item(
            "Quit without saving", "", False, True
        )

        if clicked_save:
            with open(S.save_filename, "w") as file:
                file.write(yaml.dump(obj_as_dict(S.decl)))
            imgui.open_popup("done_save")
        if imgui.begin_popup_modal("done_save")[0]:
            imgui.text(f"Saved file to '{S.save_filename}'")
            if imgui.button(label="OK", width=120, height=0):
                imgui.close_current_popup()
            imgui.set_item_default_focus()
            imgui.end_popup()

        if clicked_quit:
            exit(1)

        menu_size = imgui.get_window_size()
        imgui.end_main_menu_bar()

    # imgui.set_next_window_size(550, 680, condition=imgui.FIRST_USE_EVER)
    io = imgui.get_io()
    imgui.set_next_window_size(io.display_size.x, io.display_size.y - menu_size.y)
    imgui.set_next_window_position(0,menu_size.y)
    flags = imgui.WINDOW_NO_RESIZE | imgui.WINDOW_NO_MOVE | imgui.WINDOW_NO_COLLAPSE | imgui.WINDOW_NO_TITLE_BAR
    # flags |= imgui.WINDOW_NO_BRING_TO_FRONT_ON_FOCUS

    if not imgui.begin(label="SQL Import App", flags=flags):
        # Early out if the window is collapsed, as an optimization.
        imgui.end()
        return
        # sys.exit(1)

    steps = ["0. Introduction",
             "1. Choose Table Types",
             "2. Allocate Columns",
             "3. Finalise Data Types",
             "4. End"]

    imgui.text("Steps: ")
    width = calc_widths(len(steps))
    for step_i,step in enumerate(steps):
        imgui.same_line()

        if step_i != 0:
            imgui.text(">>>>")
            imgui.same_line()

        old_step = S.cur_step
        pop_amount = 0
        if step_i < old_step:
            imgui.push_style_color(imgui.COLOR_BUTTON, *done_step_color)
            imgui.push_style_color(imgui.COLOR_TEXT, *done_step_text_color)
            pop_amount = 2
        if step_i == old_step:
            imgui.push_style_color(imgui.COLOR_BUTTON, *active_step_color)
            pop_amount = 1
        if imgui.button(step):
            S.cur_step = step_i
        imgui.pop_style_color(pop_amount)

    imgui.spacing()
    imgui.separator()
    imgui.separator()
    imgui.spacing()

    if S.cur_step == 0:
        imgui.text_wrapped("""Please walk through the steps of this import one at a time by clicking the buttons at the top of the window. Each step is a "mode" of editing, and presents more detailed choices.

You can always go back to a previous step at any time. Hit save when you are finished.

""")
    elif S.cur_step == 1:
        imgui.text_wrapped("""For each table, select whether each of its rows represents
        a) single entity with attached fields
        b) relation connecting entities from other tables

""")
        col_selecting(S, True)
    elif S.cur_step == 2:
        imgui.text_wrapped("""For each table, choose what the purpose of each column is:
        id:       The unique identifier for this row's entity/relation
        entity:   This column is the unique identifier of an entity from different table
        field:    An attached piece of scalar data (e.g. int, float, string)
        field_on: Advanced only: a piece of scalar data to attach to the connection between two entities.
        ignore:   Don't include this column

""")
        col_selecting(S, False)
    elif S.cur_step == 3:
        imgui.text_wrapped("""You can customize the details from the previous steps, including specific Zef entity/relation names for each column and the data type to use for each column.
        
The checkbox in front of each field should be set to the ID column. This will be removed in future versions and automatically track the "id" column.

""")
        full_col_edit(S)
    elif S.cur_step == 4:
        imgui.text_wrapped(f"""If you are satisfied with the layout, you should now hit "Save" above and then load this into Zef using something like: 
        

from zef import *
from zef.ops import *
from zef.experimental import sql_import

decl = "{S.save_filename}" | load_file | run | get["content"] | collect
g = Graph()
actions = sql_import.import_actions(decl)
actions | transact[g] | run
""")
    else:
        raise Exception("Shouldn't get here")

    imgui.end()


def full_col_edit(S):
    ent_opts = []
    for d in S.decl.definitions:
        if d.kind == "entity":
            ent_opts += [d.ET]

    for d in S.decl.definitions:
        show = table_header(d, True)

        imgui.push_id(d.tag)
        if show:
            # changed, d.kind = combo_autoindex(
            #     label="Kind",
            #     current=d.kind,
            #     items=["entity", "relation"]
            # )
            changed = False
            if imgui.radio_button("Entity", d.kind == "entity"):
                d.kind = "entity"
                changed = True
            same_line()
            if imgui.radio_button("Relation", d.kind == "relation"):
                d.kind = "relation"
                changed = True
            if changed:
                d.ID_col = get(d, "ID_col", None)
                if d.ID_col is None:
                    # Guess
                    for item in d.cols:
                        if item.purpose == "id":
                            d.ID_col = item.name
                            break
                if d.ID_col is None:
                    for item in d.cols:
                        if item.purpose == "entity":
                            d.ID_col = item.name
                            break
                if d.kind == "entity":
                    d.ET = get(d, "ET", get(d, "RT", pascalcase(d.tag)))
                if d.kind == "relation":
                    d.RT = get(d, "RT", get(d, "ET", pascalcase(d.tag)))
                for key in set(keys(d)).difference(kind_keys[d.kind]):
                    delattr(d, key)

            same_line()
            if d.kind == "entity":
                width = calc_widths(1)
                changed,d.ET = raet_input("ET", d.ET, width)

            if d.kind == "relation":
                width = calc_widths(1)
                changed,d.RT = raet_input("RT", d.RT, width)
                
            
            field_on_opts_first = [item.name for item in d.cols if item.purpose == "entity" and item.name != get(d, "ID_col", None)]
            field_on_opts_second = [item.name for item in d.cols if item.name not in field_on_opts_first and item.name != get(d, "ID_col", None)]
            for item in d.cols:
                tree_name = f"{item.purpose.upper()}: {item.name}"
                if item.purpose == "entity":
                    tree_rest = f"ET.{item.ET} using RT.{item.RT}"
                elif item.purpose == "field":
                    tree_rest = f"AET.{item.data_type} using RT.{item.RT}"
                elif item.purpose == "field_on":
                    rt_text = get_col(d.cols, item.target)
                    if rt_text is None:
                        rt_text = "ERROR"
                    else:
                        rt_text = f"RT.{rt_text.RT}"
                    tree_rest = f"AET.{item.data_type} from {item.target} ({rt_text}) using RT.{item.RT}"
                else:
                    tree_rest = ""

                if d.kind == "entity" or d.kind == "relation":
                    checked = d.ID_col == item.name
                    changed,checked = imgui.checkbox(f"###{item.name}Checkbox", checked)
                    if changed:
                        if d.kind == "entity":
                            d.ID_col = item.name
                        elif d.kind == "relation":
                            if checked:
                                d.ID_col = item.name
                            else:
                                d.ID_col = None
                    same_line(5)

                if item.purpose == "entity":
                    color = entity_foreground
                elif item.purpose in ["source", "target", "id"]:
                    color = special_foreground
                else:
                    color = (1.0, 1.0, 1.0)
                imgui.push_style_color(imgui.COLOR_TEXT, *color)
                opened = imgui.tree_node(f"{tree_name} -- {tree_rest}" + "###" + item.name)
                imgui.pop_style_color()
                if opened:
                    with ItemWidth(100):
                        changed, new_purpose = combo_autoindex(
                            label="###Purpose",
                            current=item.purpose,
                            items=purpose_opts
                        )
                    if changed:
                        # We need to update the layout of the struct
                        change_col_to_purpose(item, new_purpose)

                    if item.purpose == "entity":
                        imgui.same_line()
                        width = calc_widths(2)
                        # changed, item.ET = raet_input("ET", item.ET, width)
                        changed, item.ET = entity_selector((d.tag, item.name), item.ET, width, ent_opts)
                        same_line()
                        label = "RT"
                        changed, item.RT = raet_input(label, item.RT, width)
                    elif item.purpose == "field":
                        same_line()
                        width = calc_widths(2)
                        changed, item.RT = raet_input("RT", item.RT, width)
                        same_line()
                        item.data_type = data_type_selector(item.data_type, width)
                    elif item.purpose == "field_on":
                        same_line()
                        width = calc_widths(3)
                        changed, item.RT = raet_input("RT", item.RT, width)
                        same_line()
                        with ItemWidth(width, True):
                            changed, new_target = combo_autoindex(label="Target", current=item.target, items=field_on_opts_first + ["---"] + field_on_opts_second)
                            if changed and new_target != "---":
                                item.target = new_target
                        same_line()
                        item.data_type = data_type_selector(item.data_type, width)
                    elif item.purpose == "ignore":
                        pass
                    elif item.purpose in ["source", "target"]:
                        imgui.same_line()
                        width = calc_widths(1)
                        changed, item.ET = entity_selector((d.tag, item.name), item.ET, width, ent_opts)
                    elif item.purpose == "id":
                        same_line()
                        width = calc_widths(2)
                        changed, item.RT = raet_input("RT", item.RT, width)
                        same_line()
                        item.data_type = data_type_selector(item.data_type, width)
                    else:
                        raise NotImplementedError()
                    imgui.tree_pop()

        imgui.pop_id()



def col_selecting(S, just_tables):
    for d in S.decl.definitions:
        show = table_header(d, False)

        if show:
            imgui.push_id(d.tag)
            changed = False
            if imgui.radio_button("Entity (?)", d.kind == "entity"):
                d.kind = "entity"
                changed = True
            simple_tooltip("Each row in the table will be turned into a Zef entity.")
            same_line()
            if imgui.radio_button("Relation (?)", d.kind == "relation"):
                d.kind = "relation"
                changed = True
            simple_tooltip("Each row in the table represents the relation that connects two entities. These two entities should be described in other tables.")
            if changed:
                d.ID_col = get(d, "ID_col", None)
                if d.ID_col is None:
                    # Guess
                    for item in d.cols:
                        if item.purpose == "id":
                            d.ID_col = item.name
                            break
                if d.ID_col is None:
                    for item in d.cols:
                        if item.purpose == "entity":
                            d.ID_col = item.name
                            break
                if d.kind == "entity":
                    d.ET = get(d, "ET", get(d, "RT", pascalcase(d.tag)))
                if d.kind == "relation":
                    d.RT = get(d, "RT", get(d, "ET", pascalcase(d.tag)))
                for key in set(keys(d)).difference(kind_keys[d.kind]):
                    delattr(d, key)

            all_col_names = [x.name for x in d.cols]
            if d.kind == "entity":
                special_cols = ["id"]
                width = calc_widths(1) - default_label_width
                changed,d.ET = raet_input("ET", d.ET, width, "Zef name")
                simple_tooltip("Each row will generate entities of this ET in the Zef graph.")

            if d.kind == "relation":
                special_cols = ["id", "source", "target"]
                width = calc_widths(1) - default_label_width
                changed,d.RT = raet_input("RT", d.RT, width, "Zef name")
                simple_tooltip("Each row will generate relations of this RT in the Zef graph.")

            if not just_tables:
                imgui.separator()
                imgui.text("Column designations")
                imgui.separator()
                categories = [("field", "Simple data to attach to this entity/relation"),
                            ("entity", "Indicate this column is an ID for a different entity and will create a relation between that entity and this row."),
                            ("field_on", "Only for advanced use. This attaches data to a relation created from a \"entity\" column of this table."),
                            ("ignore", "Columns to ignore")]
                col_width = calc_widths(len(categories) + 1)

                def get_category_length(cat):
                    purpose,title = cat
                    return d.cols | filter[lambda x: x.purpose == purpose] | length | collect
                max_length = categories | map[get_category_length] | max | collect
                height = max(100, (max_length+2) * imgui.get_text_line_height_with_spacing())
                # First the special columns
                imgui.begin_child(f"{d.tag}special", col_width, height, flags=imgui.WINDOW_NO_SCROLL_WITH_MOUSE)
                imgui.text("Special columns")
                imgui.separator()
                for col_s in special_cols:
                    imgui.text(col_s)
                    imgui.same_line()
                    item = get_col_name_from_purpose(d.cols, col_s)
                    item_s = "<none>" if item is None else item
                    width = calc_widths(1)
                    imgui.selectable(item_s + f"###{col_s}selectable", True, width=width)
                    if item is not None:
                        if imgui.begin_drag_drop_source():
                            imgui.set_drag_drop_payload(f"{d.tag} drag", item.encode("ascii"))
                            imgui.text(item)
                            imgui.end_drag_drop_source()
                    if imgui.begin_drag_drop_target():
                        payload = imgui.accept_drag_drop_payload(f"{d.tag} drag")
                        if payload is not None:
                            new_item = payload.decode("ascii")
                            col = get_col(d.cols, new_item)
                            change_col_to_purpose(col, col_s)
                            # Reset the old one's purpose to be a generic field
                            if item is not None:
                                col = get_col(d.cols, item)
                                change_col_to_purpose(col, "field")
                        imgui.end_drag_drop_target()

                    if col_s == "id":
                        tool_s = "This sets the column to be how other tables can link to this row. An entity must have an ID but it is optional for a relation."
                    elif col_s == "source":
                        tool_s = "The column from which the source entity will be drawn. This column should match another table's ID column. This is like a SQL foreign key."
                    elif col_s == "target":
                        tool_s = "The column from which the target entity will be drawn. This column should match another table's ID column. This is like a SQL foreign key."
                    simple_tooltip(tool_s)

                    imgui.spacing()
                    imgui.spacing()
                    imgui.spacing()
                imgui.end_child()
                imgui.same_line()

                for cat_i,(cat,title) in enumerate(categories):
                    if cat_i > 0:
                        imgui.same_line()
                    imgui.begin_child(f"{d.tag}{cat}", col_width, height, border=True, flags=imgui.WINDOW_NO_SCROLL_WITH_MOUSE)
                    imgui.text(f'"{cat}"')
                    simple_tooltip(title)
                    imgui.separator()
                    items = [x.name for x in d.cols if x.purpose == cat]
                    for item in items:
                        imgui.selectable(item, False)
                        if imgui.begin_drag_drop_source():
                            imgui.set_drag_drop_payload(f"{d.tag} drag", item.encode("ascii"))
                            imgui.text(item)
                            imgui.end_drag_drop_source()
                    imgui.end_child()
                    if imgui.begin_drag_drop_target():
                        payload = imgui.accept_drag_drop_payload(f"{d.tag} drag")
                        if payload is not None:
                            item = payload.decode("ascii")
                            col = get_col(d.cols, item)
                            change_col_to_purpose(col, cat)
                        imgui.end_drag_drop_target()

            imgui.dummy(0, 20)
            imgui.pop_id()

def table_header(d, show_details):
    if d.kind == "entity":
        color = entity_background
        name_def = f"ENTITY: ET.{d.ET}"
    elif d.kind == "relation":
        color = relation_background
        if show_details:
            source_cols = [x for x in d.cols if x.purpose == "source"]
            if len(source_cols) != 1:
                source_name = "UNKNOWN"
            else:
                source_name = "ET." + source_cols[0].ET
            target_cols = [x for x in d.cols if x.purpose == "target"]
            if len(target_cols) != 1:
                target_name = "UNKNOWN"
            else:
                target_name = "ET." + target_cols[0].ET
            name_def = f"RELATION: {source_name}--RT.{d.RT}->{target_name}"
        else:
            name_def = f"RELATION: RT.{d.RT}"
    else:
        color = (1.0, 1.0, 1.0)
        name_def = "UNKNOWN"
    name = f"Table {d.tag}, {d.data_source.filename}: {name_def}"
    # imgui.push_style_color(imgui.COLOR_TEXT, *color)
    imgui.push_style_color(imgui.COLOR_HEADER, *color)
    show,_ = imgui.collapsing_header(name + "###" + d.tag, flags=imgui.TREE_NODE_DEFAULT_OPEN)
    imgui.pop_style_color()
    return show


##############################
# * Validation
#----------------------------

def todo():
    # TODO: entities can't have ID purposes
    # TODO: Exactly one source/target in a relation kind
    # TODO: Maximum one ID purpose in relation kind
    # TODO: Exactly one ID purpose 
    pass



################################
# * ImGUI helpers
#------------------------------



default_spacing = 20
default_label_width = 100

def calc_widths(N, spacing=default_spacing, full_width=None):
    if full_width is None:
        full_width = imgui.get_content_region_available_width()
    return (full_width - (N-1)*default_spacing) / N

def same_line(spacing=default_spacing):
    imgui.same_line(spacing=spacing)

from contextlib import contextmanager
@contextmanager
def ItemWidth(w, label_w=0):
    if label_w is True:
        label_w = default_label_width
    imgui.push_item_width(w-label_w)
    yield
    imgui.pop_item_width()
    imgui.set_cursor_pos_x(imgui.get_cursor_pos_x() + label_w)
    


def combo_autoindex(label, current, items, **kwds):
    if current in items:
        index = items.index(current)
    else:
        index = -1

    changed, new_val = imgui.combo(
        label=label,
        current=index,
        items=items,
        **kwds
    )
    return changed, items[new_val]

def data_type_selector(val, full_width):
    imgui.push_id("DataTypeSelector")
    if '.' in val:
        parts = val.split('.')
        typ = parts[0]
        subtyp = '.'.join(parts[1:])
    else:
        typ = val
        subtyp = None

    if subtyp is None:
        N = 1
    else:
        N = 2

    start_x = imgui.get_cursor_pos_x()
    spacing = 5

    with ItemWidth(0):
        imgui.text("AET.")
    same_line(0)

    with ItemWidth(150):
        changed, new_opt = combo_autoindex(
            label="###Combo",
            current=typ,
            items=["Bool", "Int", "Float", "String", "Time", "QuantityFloat", "QuantityInt", "Enum"]
        )
    if subtyp is not None:
        same_line(0)
        with ItemWidth(0):
            imgui.text(".")
        same_line(0)
        leftover = full_width - (imgui.get_cursor_pos_x() - start_x)
        with ItemWidth(leftover):
            changed, new_subtyp = imgui.input_text(label="###SubType", value=subtyp, buffer_length=50)
    else:
        new_subtyp = None

    if new_opt in ["QuantityFloat", "QuantityInt", "Enum"]:
        if new_subtyp is None:
            new_subtyp = ""
    else:
        new_subtyp = None

    imgui.pop_id()

    if new_subtyp is None:
        return new_opt
    else:
        return f"{new_opt}.{new_subtyp}"


def raet_input(token, value, width, label=""):
    imgui.begin_group()
    imgui.text(token + ".")
    used_width = imgui.get_item_rect_size()[0]
    same_line(0)
    with ItemWidth(width - used_width):
        changed, value = imgui.input_text(label=f"{label}###{token}{label}", value=value, buffer_length=50)
    imgui.end_group()
    return changed, value


last_was_popup_active = {}
def entity_selector(id, value, width, ent_opts):
    # Provides a dropdown selectable for choosing an entity from current ones in the list
    something_clicked = False
    
    # changed,value = imgui.input_text(label="TESTING", value=value, buffer_length=50)
    change_bg = value not in ent_opts
    if change_bg:
        imgui.push_style_color(imgui.COLOR_FRAME_BACKGROUND, 0.5,0.1,0.1)
    changed,value = raet_input("ET", value, width)
    input_active = imgui.is_item_active() or changed
    input_focused = imgui.is_item_focused()
    if change_bg:
        imgui.pop_style_color()
    if input_active or last_was_popup_active.setdefault(id, False):
        imgui.set_next_window_position(imgui.get_item_rect_min().x, imgui.get_item_rect_max().y)
        flags = imgui.WINDOW_NO_TITLE_BAR | imgui.WINDOW_NO_RESIZE | imgui.WINDOW_NO_MOVE | imgui.WINDOW_ALWAYS_AUTO_RESIZE | imgui.WINDOW_ALWAYS_VERTICAL_SCROLLBAR
        imgui.begin("popup###" + str(id), flags=flags)
        last_was_popup_active[id] = imgui.is_window_focused()
        if input_active:
            imgui.set_window_focus()
        for x in ent_opts:
            clicked, _ = imgui.selectable(x, False)
            if clicked:
                changed = True
                value = x
                something_clicked = True
        # imgui.end_tooltip()
        imgui.end()
    else:
        last_was_popup_active[id] = False

    if something_clicked:
        last_was_popup_active[id] = False

    return changed,value

def simple_tooltip(x):
    if imgui.is_item_hovered():
        imgui.begin_tooltip()
        imgui.push_text_wrap_position(400)
        imgui.text_wrapped(x)
        imgui.end_tooltip()
        # imgui.set_tooltip(x)