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

from ._core import *
from ._ops import *
from .VT import *
from .zef_functions import func

@func
def is_a_component(component, vt):
    return isinstance(component, ValueType_) and without_absorbed(component) == vt

#--------------------------TEXT--------------------------------------
def dispatch_rich_text(component):
    import rich.text as rt

    def resolve_styles(d):
        # TODO filter out non-rich styles if found
        from rich.style import Style
        if "background_color" in d:
            d['bgcolor'] = d['background_color']
            d.pop("background_color")
        return Style(**d)

    def resolve_nonstyle_attributes(d):
        new_d = {}
        if "justify" in d: new_d['justify'] = d.pop("justify")
        if "overflow" in d: new_d['overflow'] = d.pop("overflow")
        if "no_wrap" in d: new_d['no_wrap'] = d.pop("no_wrap")
        if "tab_size" in d: new_d['tab_size'] = d.pop("tab_size")

        return new_d
    
    def resolve_text_and_nested_data(t):
        pairs_or_text = []
        internals = t | absorbed | collect
        assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Text should be of type dict!"
        text = internals[0].get("text", "")
        attributes = {**internals[0]}
        attributes.pop("text", None)
        nonstyle_attributes = resolve_nonstyle_attributes(attributes)

        if "data" in attributes:
            data = attributes["data"]
            def dispatch_on_data_type(data):
                # Just a str
                if isinstance(data, str):
                    pairs_or_text.append((data, ""))

                # Nested Text component
                elif is_a_component(data, Text) : 
                    pairs_or_text.extend(dispatch_rich_text(data))
                
                # List of the above 2
                elif isinstance(data, list):
                    [dispatch_on_data_type(x) for x in data]

                else:
                    raise ValueError("Text's data field could contain only a str, Text component, or a list composed of the two.")

            dispatch_on_data_type(data)
            attributes.pop("data")
        
        if "style" in attributes:
            style = dispatch_rich_style(attributes["style"])
        else:
            style = resolve_styles(attributes)

        return [(text, style), *pairs_or_text], nonstyle_attributes

    pairs_or_text, nonstyle_attributes = resolve_text_and_nested_data(component)
    return rt.Text.assemble(*pairs_or_text, **nonstyle_attributes)



#--------------------------Code--------------------------------------
def dispatch_rich_syntax(component):
    import rich.syntax as rs

    def filter_attributes(d):
        # TODO filter out non-rich styles if found
        lexer = d.get("language", "python3")
        d.pop("language", None)
        d.pop("code", None)
        return {**d, "lexer": lexer}
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Code should be of type dict!"
    code = internals[0].get("code", "")
    attributes = {**internals[0]}
    attributes = filter_attributes(attributes)
    return rs.Syntax(code, **attributes)


#--------------------------Style--------------------------------------
def dispatch_rich_style(component):
    def resolve_styles(new_d):
        from rich.style import Style
        if "background_color" in new_d:
            new_d['bgcolor'] = new_d['background_color']
            new_d.pop("background_color")
        return Style(**new_d)
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Style should be of type dict!"
    attributes = {**internals[0]}
    return resolve_styles(attributes)



#--------------------------Table--------------------------------------
def dispatch_rich_table(component):
    import rich.table as rt
    import itertools as itr

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
        # row_styles: List of styles or strings
        if "row_styles" in d:
            d["row_styles"] = [handle_nested_components(x) for x in d["row_styles"]]
        
        # title: String or Text
        if "title" in d:
            d["title"] = handle_nested_components(d["title"])

        # cols: string or text
        if "cols" in d:
            d["cols"] = [handle_nested_components(x) for x in d["cols"]]
        
        # rows: string or text
        if "rows" in d:
            d["rows"] = [handle_nested_components(x) for x in d["rows"]]

        if "box" in d:
            d["box"] = box_constants_mapping(d["box"])

        return d
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Table should be of type dict!"
    attributes = resolve_attributes({**internals[0]})
    cols = attributes.pop('cols', [])
    rows = attributes.pop('rows', [])
    rich_table = rt.Table(*cols, **attributes)
    # [rich_table.add_column(col, style = style) for (col, style) in itr.zip_longest(cols, col_styles)]
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
    
    header = internals[0].get("text", "")
    if is_a_component(header, Text): header = dispatch_rich_text(header)
    attributes = resolve_attributes(internals[0])

    return rt.Column(header, **attributes)


#--------------------------Frame--------------------------------------
def dispatch_rich_panel(component):
    import rich.panel as rp

    def filter_and_resolve_attributes(d):
        d.pop("displayable", None)

        if "title" in d and is_a_component(d["title"], Text): 
            d["title"] = dispatch_rich_text(d["title"])
        
        if "subtitle" in d and is_a_component(d["title"], Text): 
            d["subtitle"] = dispatch_rich_text(d["subtitle"])

        if "box" in d:
            d["box"] = box_constants_mapping(d["box"])

        return d
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Code should be of type dict!"
    displayable = internals[0].get("displayable")
    displayable = match_and_dispatch(displayable)
    if not displayable: raise ValueError("Frame's displayable field wasn't passed a component!")
    
    attributes = {**internals[0]}
    attributes.pop("displayable", None)
    attributes = filter_and_resolve_attributes(attributes)
    return rp.Panel(displayable, **attributes)


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
        (Any, lambda x: print(f"Show not defined for {x}"))
    ] | collect

def print_rich(displayable):
    import rich
    console = rich.console.Console()
    displayable = match_and_dispatch(displayable)
    console.print(displayable)

show = run[print_rich]