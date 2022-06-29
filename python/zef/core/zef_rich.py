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

    def resolve_styles(new_d):
        # TODO filter out non-rich styles if found
        from rich.style import Style
        if "background_color" in new_d:
            new_d['bgcolor'] = new_d['background_color']
            new_d.pop("background_color")
        return Style(**new_d)
    
    def resolve_text_style_pairs(t):
        pairs = []
        internals = t | absorbed | collect
        assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Text should be of type dict!"
        text = internals[0].get("text", "")
        attributes = {**internals[0]}
        attributes.pop("text", None)

        if "data" in attributes:
            data = attributes["data"]
            def dispatch_on_data_type(data):
                if isinstance(data, str):
                    pairs.append((data, ""))
                # TODO Check this using is_a
                elif is_a_component(data, Text) : 
                    pairs.extend(resolve_text_style_pairs(data))
                elif isinstance(data, list):
                    [dispatch_on_data_type(x) for x in data]
                else:
                    raise ValueError
            dispatch_on_data_type(data)
            attributes.pop("data")
        
        if "style" in attributes:
            style = dispatch_rich_style(attributes["style"])
        else:
            style = resolve_styles(attributes)
        return [(text, style), *pairs]

    pairs = resolve_text_style_pairs(component)
    return rt.Text.assemble(*pairs)



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


    def handle_string_or_component(str_or_component):
        if isinstance(str_or_component, str):
            return str_or_component
        elif is_a_component(str_or_component, Text):
            return dispatch_rich_text(str_or_component)
        elif is_a_component(str_or_component, Style):
            return dispatch_rich_style(str_or_component)
        else:
            raise NotImplementedError

    def resolve_attributes(d):
        # row_styles: List of styles or strings
        if "row_styles" in d:
            d["row_styles"] = [handle_string_or_component(x) for x in d["row_styles"]]

        # title: String or Text
        if "title" in d:
            d["title"] = handle_string_or_component(d["title"])

        # cols: string or text
        if "cols" in d:
            d["cols"] = [handle_string_or_component(x) for x in d["cols"]]

        return d
    
    internals = component | absorbed | collect
    assert isinstance(internals[0], dict), "First absorbed argument for ZefUI Table should be of type dict!"
    attributes = resolve_attributes({**internals[0]})
    cols = attributes.pop('cols', [])
    rows = attributes.pop('rows', [])
    rich_table = rt.Table(**attributes)
    [rich_table.add_column(col) for col in cols]
    [rich_table.add_row(*row) for row in rows]
    return rich_table


#-------------------Dispatch-------------------------------------
def print_rich(displayable):
    import rich
    console = rich.console.Console()

    displayable = displayable | match[
        (Is[is_a_component[Text]], dispatch_rich_text),
        (Is[is_a_component[Code]], dispatch_rich_syntax),
        (Is[is_a_component[Style]], dispatch_rich_style),
        (Is[is_a_component[Table]], dispatch_rich_table),
        (Any, lambda x: print(f"Show not defined for {x}"))
    ] | collect
    
    console.print(displayable)

show = run[print_rich]