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

from . import *
from ..core import *
from ..ops import *
from ..core.zef_functions import func

@func
def is_a_component(component, vt):
    return isinstance(component, ValueType_) and without_absorbed(component) == vt

#--------------------------TEXT--------------------------------------
def dispatch_rich_text(component):
    import rich.text as rt

    def resolve_style(d):
        if "style" in d: return dispatch_rich_style(d["style"])
        return dispatch_rich_style(Style(**d))

    def resolve_attributes(d):
        allowed_keys = ["justify", "overflow", "no_wrap", "tab_size"]
        attributes = select_keys(d, *allowed_keys)
        return attributes

    def dispatch_on_data_type(data, style):
        # Just a str
        if isinstance(data, str):
            pairs_or_text.append((data, style))
        # Nested Text component
        elif is_a_component(data, Text) : 
            pairs_or_text.extend(dispatch_rich_text(data))
        # List of the above 2
        elif isinstance(data, list):
            [dispatch_on_data_type(x, style) for x in data]
        else:
            raise ValueError("Text's data field could contain only a str, Text component, or a list composed of the two.")
    
    pairs_or_text = []
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Text should be of type dict!"
    data = internals[0].get("data", "")
    attributes = resolve_attributes(internals[0])
    style      = resolve_style(internals[0])
    dispatch_on_data_type(data, style)

    return rt.Text.assemble(*pairs_or_text, **attributes)



#--------------------------Code--------------------------------------
def dispatch_rich_syntax(component):
    import rich.syntax as rs

    def resolve_attributes(d):
        allowed_keys = ["line_numbers", "theme", "tab_size", "indent_guides"]
        attributes = select_keys(d, *allowed_keys)
        attributes["lexer"] = d.get("language", "python")
        return attributes
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Code should be of type dict!"
    data = internals[0].get("data", "")
    attributes = resolve_attributes(internals[0])
    return rs.Syntax(data, **attributes)


#--------------------------Style--------------------------------------
def dispatch_rich_style(component):
    def resolve_styles(d):
        from rich.style import Style
        allowed_keys = ["bold", "italic", "strike", "underline", "overline", "color"]
        styles = select_keys(d, *allowed_keys)
        styles["bgcolor"] = d.get("background_color", None)
        return Style(**styles)
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Style should be of type dict!"
    return resolve_styles(internals[0])



#--------------------------Table--------------------------------------
def dispatch_rich_table(component):
    import rich.table as rt

    def handle_nested_components(maybe_component):
        if isinstance(maybe_component, str):
            return maybe_component
        elif is_a_component(maybe_component, Text):
            return dispatch_rich_text(maybe_component)
        elif is_a_component(maybe_component, Column):
            return dispatch_rich_column(maybe_component)
        elif is_a_component(maybe_component, Style):
            return dispatch_rich_style(maybe_component)
        elif isinstance(maybe_component, tuple):
            return tuple([handle_nested_components(el) for el in maybe_component])
        else:
            raise NotImplementedError(f"{maybe_component}:{type(maybe_component)}")

    def resolve_attributes(d):
        allowed_keys = ["row_styles", "title", "box", "expand", "show_header", "padding", "show_footer", "show_edge", "show_lines", "width"]
        attributes       = select_keys(d, *allowed_keys)
        # row_styles: List of attributes or strings
        if "row_styles" in attributes:
            attributes["row_styles"] = [handle_nested_components(x) for x in attributes["row_styles"]]
        # title: String or Text
        if "title" in attributes:
            attributes["title"] = handle_nested_components(attributes["title"])
        # box: str -> box.Attribute
        if "box" in attributes:
            attributes["box"] = box_constants_mapping(attributes["box"])
        return attributes
    
    def resolve_row_and_cols(rows, cols):
        return [handle_nested_components(x) for x in rows], [handle_nested_components(x) for x in cols]

    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Table should be of type dict!"
    rows, cols = resolve_row_and_cols(internals[0].get('rows', []), internals[0].get('cols', []))
    attributes = resolve_attributes(internals[0])

    rich_table = rt.Table(*cols, **attributes)
    [rich_table.add_row(*row) for row in rows]
    return rich_table

#--------------------------Column--------------------------------------
def dispatch_rich_column(component):
    import rich.table as rt

    def resolve_attributes(d):
        allowed_keys = ["header_style", "footer_style", "style", "justify", "vertical", "width", "min_width", "max_width", "ratio", "no_wrap"]
        attributes = select_keys(d, *allowed_keys)
        # Resolve the non-string styles if found
        for special_key in ["header_style", "footer_style", "style"]:
            if special_key in attributes and is_a_component(attributes[special_key], Style):
                attributes[special_key] = dispatch_rich_style(attributes[special_key])
        
        return attributes
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Column should be of type dict!"
    
    data = internals[0].get("data", "")
    if is_a_component(data, Text): data = dispatch_rich_text(data)
    attributes = resolve_attributes(internals[0])

    return rt.Column(data, **attributes)


#--------------------------Frame--------------------------------------
def dispatch_rich_panel(component):
    import rich.panel as rp

    def resolve_attributes(d):
        allowed_keys = ["title", "subtitle", "box", "expand", "padding"]
        attributes = select_keys(d, *allowed_keys)
        if "title" in attributes and is_a_component(attributes["title"], Text): 
            attributes["title"] = dispatch_rich_text(attributes["title"])
        
        if "subtitle" in attributes and is_a_component(attributes["subtitle"], Text): 
            attributes["subtitle"] = dispatch_rich_text(attributes["subtitle"])
        
        if "box" in attributes:
            attributes["box"] = box_constants_mapping(attributes["box"])


        return attributes
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Code should be of type dict!"
    if "data" not in internals[0]: raise ValueError("Can't render Frame without any data to display!")
    data = internals[0].get("data")
    data = match_and_dispatch(data)
    attributes = resolve_attributes(internals[0])
    return rp.Panel(data, **attributes)


#--------------------------HStack,VStack--------------------------------------
def dispatch_rich_stack(component):
    import rich.table as rt

    def handle_nested_components(maybe_component):
        if isinstance(maybe_component, str):
            return maybe_component
        else:
            return match_and_dispatch(maybe_component)

    def resolve_attributes(d):
        allowed_keys = ["expand", "padding", "pad_edge"]
        attributes = select_keys(d, *allowed_keys)
        return attributes
    
    stack_type = str(component | without_absorbed | collect)
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Table should be of type dict!"
    data = internals[0].get('data', [])
    assert isinstance(data, list), f"Data for ZefUI Stack should be of type list! {data} was passed"
    cols = internals[0].get('cols', [])
    assert isinstance(cols, list), f"Data for ZefUI Stack should be of type list! {cols} was passed"
    data = [handle_nested_components(c) for c in data]
    cols = [handle_nested_components(c) for c in cols]
    attributes = resolve_attributes(internals[0])

    rich_grid = rt.Table.grid(*cols, **attributes)
    if stack_type == "HStack":
        rich_grid.add_row(*data)
    elif stack_type == "VStack":
        [rich_grid.add_row(row) for row in data]
    return rich_grid

#--------------------------Bullet/Numbered List--------------------------------------
def dispatch_bullet_or_numbered_list(component):
        list_type = str(component | without_absorbed | collect)
        if list_type == 'BulletList':
            dispatch_type = lambda _: '- ' 
        else:
            dispatch_type = lambda i: str(i + 1) +'. '
        
        internals = component | absorbed | collect
        assert isinstance(internals[0], dict), f"First absorbed argument for ZefUI {list_type} should be of type dict!"
        data  = internals[0].get('data', [])
        
        data_modified = []
        for i, el in enumerate(data):
            if isinstance(el, str): 
                data_modified.append(dispatch_type(i) + el)
            else:
                data_modified.append(HStack(data=[dispatch_type(i), el]))

        heading = internals[0].get('heading', "")
        if heading: data_modified = data_modified | prepend[heading] | collect

        allowed_keys = ["padding", "expand"]
        attributes = select_keys(internals[0], *allowed_keys)

        return dispatch_rich_stack(VStack(data_modified, **attributes))

#--------------------------Markdown--------------------------------------
def dispatch_rich_markdown(component):
    import rich.markdown as rm

    def resolve_style(d):
        if "style" in d: return dispatch_rich_style(d["style"])
        return dispatch_rich_style(Style(**d))
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Table should be of type dict!"
    style = resolve_style(internals[0])

    return rm.Markdown(component_to_markdown_string(component), style = style)


def component_to_markdown_string(component):
    
    def dispatch_bullet_or_numbered_list(component):
        list_type = str(component | without_absorbed | collect)
        if list_type == 'BulletList':
            dispatch_type = lambda i: '- ' 
        else:
            dispatch_type = lambda i: str(i + 1) +'. '
        nl = '\n'
        
        internals = component | absorbed | collect
        assert isinstance(internals[0], dict), f"First absorbed argument for ZefUI {list_type} should be of type dict!"
        data  = internals[0].get('data', [])
        heading = internals[0].get('heading', "")
        heading_level = internals[0].get('heading_level', 3)
        
        if heading: heading = f"{'#' * heading_level} {heading}"
        
        return f"""
{heading}
{nl.join([f'{dispatch_type(i)}{e}' for i,e in enumerate(data)])}
    """

    def dispatch_paragraph(component):
        internals = component | absorbed | collect
        assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Paragraph should be of type dict!"
        data  = internals[0].get('data', "")
        heading = internals[0].get('heading', "")
        heading_level = internals[0].get('heading_level', 2)
        if heading: heading = f"{'#' * heading_level} {heading}"

    
        return f"""
{heading}
{data}
    """

    return component | match[
        (Is[is_a_component[BulletList]], dispatch_bullet_or_numbered_list),
        (Is[is_a_component[NumberedList]], dispatch_bullet_or_numbered_list),
        (Is[is_a_component[Paragraph]], dispatch_paragraph),
    ] | collect


#-------------------Dispatch-------------------------------------
def box_constants_mapping(box_style: str):
    from rich import box
    str_to_constant = {
        'ascii':                   box.ASCII,
        'square':                  box.SQUARE,                
        'minimal':                 box.MINIMAL,  
        'minimal_heavy_head':      box.MINIMAL_HEAVY_HEAD,     
        'minimal_double_head':     box.MINIMAL_DOUBLE_HEAD,           
        'simple':                  box.SIMPLE,    
        'simple_head':             box.SIMPLE_HEAD,    
        'heavy':                   box.HEAVY,                 
        'heavy_edge':              box.HEAVY_EDGE,             
        'heavy_head':              box.HEAVY_HEAD,
        'double':                  box.DOUBLE,               
        'double_edge':             box.DOUBLE_EDGE,
        'simple_heavy':            box.SIMPLE_HEAVY,            
        'horizontals':             box.HORIZONTALS,              
        'rounded':                 box.ROUNDED,
    }
    return str_to_constant.get(box_style, box.ROUNDED)

def match_and_dispatch(component):
    return component | match[
        (Is[is_a_component[Text]], dispatch_rich_text),
        (Is[is_a_component[Code]], dispatch_rich_syntax),
        (Is[is_a_component[Style]], dispatch_rich_style),
        (Is[is_a_component[Table]], dispatch_rich_table),
        (Is[is_a_component[Column]], dispatch_rich_column),
        (Is[is_a_component[Frame]], dispatch_rich_panel),
        (Is[is_a_component[HStack]], dispatch_rich_stack),
        (Is[is_a_component[VStack]], dispatch_rich_stack),
        (Is[is_a_component[Paragraph]], dispatch_rich_markdown),
        (Is[is_a_component[BulletList]], dispatch_bullet_or_numbered_list),
        (Is[is_a_component[NumberedList]], dispatch_bullet_or_numbered_list),
    ] | collect

def print_rich(displayable):
    try:
        import rich
    except:
        raise ImportError("Please install rich: pip install rich")
    console = rich.console.Console(width = width_or_default())
    displayable = match_and_dispatch(displayable)
    console.print(displayable)

def to_rich_str_imp(displayable):
    try:
        import rich
    except:
        raise ImportError("Please install rich: pip install rich")

    # Create a console first to cheat the colour-detection
    # console_for_color = rich.console.Console(width = 160)
    # color_system = console_for_color.color_system
    # console = rich.console.Console(width = width_or_default(), color_system=color_system, force_jupyter=False, force_terminal=True, force_interactive=False, no_color=False)
    console = rich.console.Console(width = 160)
    displayable = match_and_dispatch(displayable)
    with console.capture() as capture:
        console.print(displayable)
    return capture.get()
    console.print(displayable)
    return output.getvalue()

def width_or_default():
    # Returns None when columns are present, to allow rich to autodetect
    #
    # Note: not that useful for everything I've tried. Really need to query dynamically instead. See:
    # - https://stackoverflow.com/questions/1780483/lines-and-columns-environmental-variables-lost-in-a-script
    # - https://stackoverflow.com/questions/1286461/can-i-find-the-console-width-with-java
    import os
    if "COLUMNS" in os.environ:
        return None
    else:
        return 160

show = run[print_rich]
displayable = run[match_and_dispatch]
to_rich_str = run[to_rich_str_imp]