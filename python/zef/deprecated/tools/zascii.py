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

__all__ = [
    "parse_zascii_to_asg",
    "draw_zascii_segmentation",
]

# ZASCII: Zuid Afrikaansche Standard Code for Information Interchange
# or:     zef ascii


# TODO: restructure the functions somewhat to deal with custom DSLs.
# ZefUI should just be one of the DSLs, but currently 'parse_zascii_to_asg'
# Is already aware of the ZefUI syntax.

# Think of this as a data pipeline:
#                   parse_zascii_to_asg                          asg_to_zefui_graph_delta
#  (zascii string)------------------------>(primitive asg: dict)----------------------------->(zefui interpreted GraphDelta)
#
# Or does it make sense to have more intermediate stages?
# also one can simply create a composite function for convenience


# Proposed changes: 
# instead of parse_zascii_to_asg already interpeting ZefUI elements and returning e.g.
# {...
#   'temp_id_0016': {'type': 'ZEFUI',
#   'zefui_type': 'On-change',
#   'value': 'on_icon_val_change',
#   'length': 21,
#   'x': 75,
#   'y': 19}
# ...}
#
# It can just return plain "elements" that are interpreted during a separate stage.
# Clustering is perfromed even later: a graph delta can only be performed after this step.




#%%
import zef.ops as ops
from typing import List, Tuple, Dict, Callable

move_up = lambda c: (c[0]-1, c[1])              # It is not a valid zascii image if we would have to step out of bounds
move_down = lambda c: (c[0]+1, c[1])
move_left = lambda c: (c[0], c[1]-1)
move_right = lambda c: (c[0], c[1]+1)

source_dir = {
    move_left: 'r',
    move_right: 'l',
    move_up: 'd',
    move_down: 'u',
}


"""
rules are used in transitions between step states: ((7,9), 'd')  means: coming from 'down', let's assume the char found at char_val((7,9)) -> '├'. 
Then the output should be  [
    ((8,9), 'l'),
    ((7,8), 'd'),      
    ]
"""
step_rules = {
    ('-', 'l'): [move_right],
    ('─', 'l'): [move_right],
    ('-', 'r'): [move_left],
    ('─', 'r'): [move_left],
    
    ('│', 'u'): [move_down],
    ('|', 'u'): [move_down],
    ('│', 'd'): [move_up],
    ('|', 'd'): [move_up],
    
    ('└', 'r'): [move_up],
    ('└', 'u'): [move_right],
    
    ('┘', 'l'): [move_up],
    ('┘', 'u'): [move_left],
    
    ('┌', 'r'): [move_down],
    ('┌', 'd'): [move_right],
    
    ('┐', 'l'): [move_down],
    ('┐', 'd'): [move_left],
    
    ('├', 'd'): [move_right, move_up],
    ('├', 'r'): [move_down, move_up],
    ('├', 'u'): [move_down, move_right],
    
    ('┤', 'd'): [move_left, move_up],
    ('┤', 'l'): [move_down, move_up],
    ('┤', 'u'): [move_down, move_left],
    
    ('┬', 'd'): [move_left, move_right],
    ('┬', 'l'): [move_down, move_right],
    ('┬', 'r'): [move_down, move_left],
    
    ('┴', 'u'): [move_left, move_right],
    ('┴', 'l'): [move_up, move_right],
    ('┴', 'r'): [move_up, move_left],    
    
    ('┼', 'd'): [move_left, move_right, move_up],
    ('┼', 'u'): [move_left, move_right, move_down],
    ('┼', 'l'): [move_up, move_right, move_down],
    ('┼', 'r'): [move_up, move_left, move_down], 
}

walk_along = {'d': move_down, 'u': move_up, 'r': move_right, 'l': move_left }
walk_opposite = {'u': move_down, 'd': move_up, 'l': move_right, 'r': move_left }
opposite = {'u': 'd', 'd': 'u', 'l': 'r', 'r': 'l'}

def make_generate_temp_uid(elements: dict):
    """returns a function that gives temp_uids and has an internal counter"""
    _tmp_id_count = 0
    def generate_tmp_id() -> str:
        nonlocal _tmp_id_count
        _tmp_id_count += 1
        candidate = f"temp_id_{_tmp_id_count:04}"    # pad with zeros, if the int is larger, it will just expand
        while candidate in elements:
            _tmp_id_count += 1
            candidate = f"temp_id_{_tmp_id_count:04}"
        return candidate
    
    return generate_tmp_id



def parse_all_expressions(s: list, character_candidates: List[Tuple[Tuple[int,int],str]], user_specified_patterns: list):    
    """
    extract all elements that are not lines.    
    character_candidates: a list of elements ((9, 54), 'R')  candidates for characters that could be part of a text element.     
    sample:
    user_specified_patterns = [
        ( lambda s: s[:3] == 'ET.',  lambda s: {'type': 'ET', 'value': s[3:]} ),
    ]
    
    result should be of form: 
    [
        {'type': RT, 'value': 'RT.Selected', 'x': 21, 'y': 3},
        {'type': str, 'value': 'Bob', 'x': 24, 'y': 7},
    ]
    """        
    # do the following imperatively
    visited_chars = {}    # will contain {(4,7): 'tmp_id_41'}    for a given coordinate: which expression or edge is occupying it?
    elements = {}    
    generate_tmp_id = make_generate_temp_uid(elements)        
    def parse_expression(ss: str) -> dict:
        """
        "some string in here"

        RT.UsedBy
        ET.Machine
        AET.String
        False
        42
        42.1
        QuantityFloat(17.5, kilogram)
        17.5 kilogram   # allow this?  
        # some comment of this type. It would be terminated by double space
        o           # an o-terminator
        -, +, ~ ZEFUI On-
        [*] allow multiple
        !   is required
        """                

        def str_represents_int(input_string: str) -> bool:
            if not input_string:   raise SyntaxError(f'Error parsing an empty expression is invalid')
            chars = set("+-0123456789")
            return all(c in chars for c in input_string)
        
        def str_represents_float(input_string: str) -> bool:
            if not input_string:  raise SyntaxError(f'Error parsing an empty expression is invalid')
            chars = set("+-.0123456789")
            return all(c in chars for c in input_string)
        
        pattern_match_rules = [
            ( lambda s: s[:3] == 'ET.',  lambda s: {'type': 'ET', 'value': s[3:]} ),
            ( lambda s: s[:3] == 'RT.',  lambda s: {'type': 'RT', 'value': s[3:]} ),
            ( lambda s: s[:4] == 'AET.',  lambda s: {'type': 'AET', 'value': s[4:]} ),
            ( lambda s: s in ('false', 'False'),  lambda s: {'type': 'Bool', 'value': False } ),
            ( lambda s: s in ('true', 'True'),  lambda s: {'type': 'Bool', 'value': True } ),
            ( str_represents_int,  lambda s: {'type': 'Int', 'value': int(s)} ),
            ( str_represents_float,  lambda s: {'type': 'Float', 'value': float(s)} ),
            ( lambda s: s[:1] == '-',  lambda s: {'type': 'ZEFUI', 'zefui_type': 'On-termination', 'value': s[1:]} ), 
            ( lambda s: s[:1] == '+',  lambda s: {'type': 'ZEFUI','zefui_type': 'On-instantation', 'value': s[1:]} ), 
            ( lambda s: s[:1] == '~',  lambda s: {'type': 'ZEFUI','zefui_type': 'On-change', 'value': s[1:]} ), 
            ( lambda s: s[:1] == '!',  lambda s: {'type': 'ZEFUI','zefui_type': 'IsRequired', 'value': '!'} ), 
            ( lambda s: s[:3] == '[*]',  lambda s: {'type': 'ZEFUI','zefui_type': 'AllowMultiple', 'value': 'AllowMultiple'} ), 
            ( lambda s: s[:1] == 'o',  lambda s: {'type': 'O-Terminator', 'value': 'o'} ),
        ]

        for predicate, result_fct in user_specified_patterns + pattern_match_rules:
            if predicate(ss): return result_fct(ss)

        # if none of the above rules match, it is invalid zascii syntax
        raise SyntaxError(f'Error parsing expression in zascii string for expression: "{ss}"')

    
    def gather_string_expression(c: Tuple[int,int], char: str):
        chars_signalling_expr_end = ('|','│','-','─','┌','┐','└','┘','├','┤','┬','┴','┼','►','◄','▼','▲','>','<')
        my_str = s[c[0]][c[1]:] # we're not sure about where this ends yet
        
        this_tmp_id = generate_tmp_id()
        
        # is it a string expression?
        if my_str[0] == '"':
            pos = my_str[1:].find('"')
            for d in range(pos+2):                
                visited_chars[ (c[0], c[1]+d) ] = this_tmp_id
            return this_tmp_id, {'type': 'String', 'length': pos+2, 'value': my_str[1:pos+1], 'x': c[1], 'y': c[0]}
                
        # a comment is delimited by two space
        if my_str[0] == '#':
            first_double_wspace = my_str[1:].find('  ')
            pos = len(my_str) if first_double_wspace == -1 else first_double_wspace
            for d in range(pos+2):                
                visited_chars[ (c[0], c[1]+d) ] = this_tmp_id
            return this_tmp_id, {'type': 'Comment', 'length': pos, 'value': my_str[0:pos+1], 'x': c[1], 'y': c[0]}
                
        # anything enclosed in parentheses:   e.g. (-42)
        if my_str[0] == '(':
            pos = my_str[1:].find(')')
            for d in range(pos+2):                
                visited_chars[ (c[0], c[1]+d) ] = this_tmp_id
            return this_tmp_id, {**parse_expression(my_str[1:pos+1]), 'length': pos+2, 'x': c[1], 'y': c[0]}      

        # anything else
        expr_length = len(my_str | ops.take_while[lambda c: c != ' ' and c not in chars_signalling_expr_end] | ops.collect)
        for d in range(expr_length):                
            visited_chars[ (c[0], c[1]+d) ] = this_tmp_id
        return this_tmp_id, {**parse_expression(my_str[:expr_length]), 'length': expr_length, 'x': c[1], 'y': c[0]}
    
    # mutaty here: gather_string_expression inserts key value pairs into visited_chars within the loop
    for el in character_candidates:        
        if el[0] not in visited_chars:
            key, val = gather_string_expression(*el)
            elements[key] = val    
    return elements, visited_chars



    
    

def is_terminal_element(char, direction) -> bool:
    if char not in set(('►','◄','▼','▲','>','<')):
        return False    
    if (char, direction) not in set(( ('►', 'l'), ('◄','r'), ('▼','u'), ('▲', 'd'), ('>','l'), ('<','r'))):
        raise RuntimeError('Terminal character found which was not attached in the right direction')    
    return True


def path_iteration_step(state: dict, s: List[str], visited_chars: dict) -> dict:
    """ input and output are of form: 
    state: {
     'path_elements': [(5,6),(5,7),(5,8), ],
     'start_points': [((15,3), 'd'),],        # there may be multiple paths merged into one set
     'end_points': [((5,10), 'u'), ((20,10), 'u')],
     
     
     'frontier': [
         ((5,7), 'd'),          # also contains the direction one is walking in!
         ((13,3), 'r'),
     ]

     'path_group_id': path_group_id_01   # Is the id of all paths that belong that share the same start or same end.
    }    
    """        
    
    frontier = state['frontier']
    if frontier == []: 
        return None
        
    current_frontier_coords, direction = frontier[0]
    y, x = current_frontier_coords
    char = s[y][x]
    remaining_frontier = frontier[1:]

    """
    Possibly a path through a path if the previous and next characters 
    match directions and current character breaks that path.
    i.e: │
     ─────────
         │
    If the previous and new character fullfil allowe characters,
    set current_coords as the next step while skipping the current character.
    """
    if char in ['|','│', '-','─',]: 
        # TODO: There could possibly be more characters to allow? Like path junctions?
        prev_and_next_char_allowed_chars = {  
            "|" : ['-','─', '└', '┘', '┌' ,'┐'],
            "│" : ['-','─', '└', '┘', '┌' ,'┐'],
            "-":  ['|','│', '└', '┘', '┌' ,'┐'],
            "─":  ['|','│', '└', '┘', '┌' ,'┐']
        }[char]
        y_prev, x_prev = walk_along[direction](current_frontier_coords)
        previous_char = s[y_prev][x_prev]
        y_next, x_next =  walk_opposite[direction](current_frontier_coords)
        next_char = s[y_next][x_next]
        if previous_char in prev_and_next_char_allowed_chars and next_char in prev_and_next_char_allowed_chars:
            # Skip current char and set current step as the next step in the path
            current_frontier_coords = (y_next, x_next) 
            visited_chars[current_frontier_coords] = state['path_group_id']
            char = next_char
            return{
                    'path_elements': [*state['path_elements'], (y_next, x_next)],
                    'start_points': state['start_points'],
                    'end_points': state['end_points'],
                    'frontier': [*[(f(current_frontier_coords), source_dir[f]) for f in step_rules[(char, direction)]], *remaining_frontier ],
                    'path_group_id': state['path_group_id'],
                }
             


    if (y,x) in visited_chars:  # we have walked off the start of the edge onto an expression
        if char in {'◄','<','▲'}:
            start_pts = []
            end_pts = [((y, x), direction), ]
            next_frontier = [(walk_along[direction](current_frontier_coords), opposite[direction])]
            visited_chars[current_frontier_coords] = state['path_group_id']
        else:
            y_next, x_next = walk_along[direction](current_frontier_coords)
            next_char = s[y_next][x_next]     #what would this be for the next step? We need to detect whether we are hitting an arrowhead 
            start_pts = [((y_next, x_next), direction), ]
            end_pts = []
            next_frontier = []
                
        return {
            'path_elements': state['path_elements'],
            'start_points': state['start_points'] + start_pts,         #Note: this is the point still on the arrow. Having the direction will be used later
            'end_points': state['end_points'] + end_pts,
            'frontier': remaining_frontier + next_frontier,
            'path_group_id': state['path_group_id'],
        }
        # is there a character that is an arrow tip and definitely terminates the path?
    if is_terminal_element(char, direction):
        visited_chars[current_frontier_coords] = state['path_group_id']
        return {
            'path_elements': [*state['path_elements'], current_frontier_coords],
            'start_points': state['start_points'],
            # we want to move one beyond the actual arrow terminating symbol: walk in the direction we came from
            # 'end_points': [*state['end_points'], walk_opposite[direction](current_frontier_coords)],      
            'end_points': [*state['end_points'], (current_frontier_coords, direction)],
            'frontier': remaining_frontier,
            'path_group_id': state['path_group_id'],
        }
    
    # if we are here' it is just one more piece of the path. Extend the frontier
    visited_chars[current_frontier_coords] = state['path_group_id']
    res = {
            'path_elements': [*state['path_elements'], current_frontier_coords],
            'start_points': state['start_points'],
            'end_points': state['end_points'],
            'frontier': [*[(f(current_frontier_coords), source_dir[f]) for f in step_rules[(char, direction)]], *remaining_frontier ],
            'path_group_id': state['path_group_id'],
        }
    return res
    
    

    
def collect_path_groups(s: List[str], visited_chars: dict, edge_char_candidates: List[Tuple[Tuple[int,int],str]], elements: dict ):
    """
    # output for this function is the set of all paths found
    [
    { 
     'path_elements': [(5,6),(5,7),(5,8), ],
     'start_points': [((15,3), 'u'),],        # there may be multiple paths merged into one set
     'end_points': [(5,10), (20,10)],
     'path_group_id': 'abcd123',
    },
    ]
    # if the result contains more than one start point and more than one end point, the compositional structure is not clear and the result is invalid.
    
    """
    generate_tmp_id = make_generate_temp_uid(elements)      # creates the function which then knows about elements and has an internal counter
    
    # Based on the selected character sets all the crawling directions
    def make_seed(s: List[str], coord: Tuple[int, int]):
        """
        return value of form [ ((5,7), 'd'),  ((21,18), 'r')]
        the second element corresponds to the direction one is coming from        
        """
        this_char = s[coord[0]][coord[1]]
        if this_char in {'◄', '<'}:
            return [(coord, 'r')]
        elif this_char == '▲':
            return [(coord, 'd')]
        
        fcts = {
            '-': (move_left, move_right),
            '─': (move_left, move_right),
            '│': (move_down, move_up),
            '|': (move_down, move_up),
            '┌': (move_down, move_right),
            '┐': (move_down, move_left),
            '└': (move_up, move_right),
            '┘': (move_up, move_left),
            '┬': (move_down, move_right, move_left),
            '┴': (move_up, move_right, move_left),
            '├': (move_down, move_up, move_right),
            '┤': (move_down, move_up, move_left),
            '┼': (move_down, move_up, move_right, move_left),
            # '▲': (lambda x: x, ),
            # '◄': (lambda x: x, ),
            # '<': (lambda x: x, ),
            # '▼': (move_up, ),
            # '►': (move_left, ),
            # '>': (move_left, ),
            }       
        return [(f(coord), source_dir[f]) for f in fcts[this_char]]
        
    # curry in visited_chars: this will be appended to within the iteration_step function!
    iteration_step = lambda state: path_iteration_step(state, s, visited_chars)    
    path_group_count = 0
    for coord, char in edge_char_candidates | ops.filter[lambda p: p[1]!='o']:
        if coord in visited_chars: continue
        path_group_count += 1
        this_tmp_id = f"path_group_{path_group_count}"
        visited_chars[coord] = this_tmp_id  #add this to the dict here
        elements[this_tmp_id] = ({
            'path_elements': [coord],
            'start_points': [],
            'end_points': [],
            'frontier': make_seed(s, coord),            # this is a vector of the form [ ((5,7), 'd'),  ] where the char indicates the direction from which the crawler came!
            'path_group_id': this_tmp_id
        } | ops.iterate[iteration_step] 
          | ops.take_while[lambda d: d is not None] 
          | ops.collect 
          | ops.last 
          | ops.insert['type', 'path_group']     # add the type key to the dict
          | ops.collect 
        )
    return elements

    
    



def draw_zascii_segmentation(visited_chars: dict):
    """ascii drawing assigning a unique single char to any entity.
    Chars range from 0 to z
    """
    def int_to_char_key(n:int)->str:
        if n<10: return str(n)
        else: return chr(87+n)  # the 'a' char can be generated from 97: int
            
    m_max = max((p[0] for p in visited_chars.keys()))
    n_max = max((p[1] for p in visited_chars.keys()))    
    keys = {p[1]: int_to_char_key(p[0]) for p in enumerate(set(visited_chars.values()))}
    dbl_arr = [[' ' for _ in range(n_max+3)] for _ in range(m_max+2)]
    for ((m,n),kk) in visited_chars.items():        
        dbl_arr[m][n] = keys[kk]
    canvas = [''.join(ro) for ro in dbl_arr] 
    for ro in canvas:
        print(ro)




def directed_path_crawl_step(d: dict, s: List[str], visited_chars: dict, point_to_be_found: str) -> dict:
    """ homomorphism on a state of shape
    {
        'path_group_id': 'abcde123',
        'active_paths': [
            {
                'path': [(3,5),(4,5),(5,5)],                 
                'next_step': ((6,5), 'd')     # last el is the direction where we came from
            },
        ],
        'completed_paths': [
            { 'type': 'Edge', 'start_point': ((12, 58), 'r'), 'end_point': ((12, 74), 'l'), 'path': [(12, 58), (12, 59), (12, 60), (12, 61), (12, 62), (12, 63)], }
        ]
    }
    """
    if d['active_paths'] == []:
        return None
    
    curr_path = d['active_paths'][0]    # current single path to extend. The step may yield multiple paths though: a list. If paths complete, the list is empty
    ((y,x), direction) = curr_path['next_step']
    char = s[y][x]

    # Checking if the current char is still part of the path_group the char belongs to
    if visited_chars[(y,x)] == d['path_group_id'] and (char, direction) in step_rules:
        paths_from_step = [ {'path': curr_path['path']+[(y,x)], 'next_step': (f((y,x)), source_dir[f]) } for f in step_rules[(char, direction)]]
        completed_paths_from_step = [] #TODO
    else:
        paths_from_step = []
        completed_paths_from_step = [{                        
            point_to_be_found: ((y,x), direction),
            'path': curr_path['path']+[(y,x)]
        }]
        #TODO check if end_point belongs to the edge
        
    return {
        'path_group_id': d['path_group_id'],
        'active_paths': paths_from_step + d['active_paths'][1:],
        'completed_paths': completed_paths_from_step + d['completed_paths']
    }
    
   

    
def split_pathgroup_into_individual_paths(path_group: dict, visited_chars: dict, s: List[str]) -> list:
    """ returns a list of edges (each edge is one unique path)

    Args:
        path_group (dict): [description]
    """    
    # construct start state from the one with one el    
    if len(path_group['start_points']) == 1 and len(path_group['end_points']) == 1:
        start_pt = path_group['start_points'][0]
        end_pt = path_group['end_points'][0]
        return [ {
            'type': 'Edge', 
            'source': visited_chars[walk_opposite[start_pt[1]](start_pt[0])],
            'target': visited_chars[walk_opposite[end_pt[1]](end_pt[0])],
            # 'out_edges': [],              #TODO: on zef graphs edges can themselves have incoming or outgoing edges
            # 'in_edges': [], 
            'start_point': start_pt,
            'end_point': end_pt,
            'path': path_group['path_elements'],
            'belongs_to': path_group['path_group_id']
            }]
    if len(path_group['start_points']) == 0 or len(path_group['end_points']) == 0:
        raise RuntimeError(f'Error attempting to split path group into individual paths with only edge {"heads" if len(path_group["end_points"]) == 0 else "tails"}: the graphical representation is ambiguous')
    
    if len(path_group['start_points']) > 1 and len(path_group['end_points']) > 1:
        raise RuntimeError('Error attempting to split path group into individual paths with multiple start and multiple end points: the graphical representation is ambiguous')
    
    
    if len(path_group['start_points']) == 1 and len(path_group['end_points']) > 1:
        (y,x), direction = path_group['start_points'][0]
        
                
        def add_source_target(d: dict) -> dict:
            start_pt = d['start_point']
            end_pt = d['end_point']
            return {
                'source': visited_chars[walk_opposite[start_pt[1]](start_pt[0])],
                'target': visited_chars[walk_opposite[end_pt[1]](end_pt[0])],
                **d
            }        
        return ({
            'path_group_id': path_group['path_group_id'],
            'active_paths': [
                {'path': [], 'next_step': ((y,x), opposite[direction]) },    # init with first step on one path
            ],
            'completed_paths': []
            } 
            | ops.iterate[lambda d: directed_path_crawl_step(d, s, visited_chars, 'end_point')]
            | ops.take_while[lambda x: x is not None]
            | ops.collect 
            | ops.last
            | ops.get['completed_paths']
            | ops.map[lambda d_path: {'type': 'Edge', 'start_point': path_group['start_points'][0],'belongs_to': path_group['path_group_id'], **d_path}]
            | ops.map[add_source_target]
            | ops.collect 
            )
    
    if len(path_group['start_points']) > 1 and len(path_group['end_points']) == 1:
        (y,x), direction = path_group['end_points'][0]
        (y,x) = walk_along[direction]((y,x)) # Go back one step because (y,x) is an arrow tip

        def add_source_target(d: dict) -> dict:
            start_pt = d['start_point']
            end_pt = d['end_point']
            return {
                'source': visited_chars[start_pt[0]],
                'target': visited_chars[walk_along[opposite[end_pt[1]]](end_pt[0])],
                **d
            }        
        return ({
            'path_group_id': path_group['path_group_id'],
            'active_paths': [
                {'path': [], 'next_step': ((y,x), opposite[direction]) },    # init with first step on one path
            ],
            'completed_paths': []
            } 
            | ops.iterate[lambda d: directed_path_crawl_step(d, s, visited_chars, 'start_point')]
            | ops.take_while[lambda x: x is not None]
            | ops.collect 
            | ops.last
            | ops.get['completed_paths']
            | ops.map[lambda d_path: {'type': 'Edge', 'end_point': path_group['end_points'][0], 'belongs_to': path_group['path_group_id'], **d_path}]
            | ops.map[add_source_target]
            | ops.collect 
            )



def calc_multi_segmentation_map(elements_and_edges: dict)->dict:
    """returns a dictionary of coordinates (keys) to a list (values), where the list 
    contains all elements that occupy the the space at the coordinate. There may be multiple, 
    e.g. in the case of a path group, multiple paths may overlap in the shared section.
    """    
    def map_to_ids_only_in_list(key_and_list: Tuple):
        key, list_of_triples = key_and_list
        return (key, list_of_triples | ops.map[lambda trip: trip[2]] | ops.collect ) 
    
    def get_coordinates(type: str) -> Callable[[str, Dict], List]:
        if type=='Edge':
            return lambda id, d: [(*c, id) for c in d['path']]
        else:
            # all other cases: {'String', 'Comment', 'ET', 'RT', 'AET', 'Int', 'Float', 'Bool'} 
            return lambda id, d: [(d['y'], d['x']+c, id) for c in range(d['length'])]    
            
    return dict(
    list(elements_and_edges.items()) 
    | ops.map[lambda p: (get_coordinates(p[1]['type']))(p[0], p[1]) ]
    | ops.concat 
    | ops.group_by[lambda triple: triple[0:2]][None]        # let the group by figure out the groups
    | ops.map[map_to_ids_only_in_list]
    | ops.collect
    )
    


def calc_halo(id: str, d: dict, seg_map: dict) -> List[Tuple[int, int]]:
    if d['type'] != 'Edge': raise NotImplementedError('calc_halo only implemented for Edges for now')

    def all_neighbors(c):
        return [f(c) for f in (move_left, move_right, move_up, move_down)]
    
    path_as_set = set(d['path'])
    return (
     d['path']
     | ops.filter[lambda p: p!=d['start_point'][0] and p!=d['end_point'][0]]        # if we calculate the neighbors: make sure not include points beyond the edge
     | ops.filter[lambda p: p in path_as_set and len(seg_map[p])==1]                # avoid parts of the pathgroup where multiple edges implicitly lie above each other
     | ops.map[all_neighbors]
     | ops.concat
     | ops.func[lambda v: set(v)]                                                   # make unique
     | ops.filter[lambda p: p not in path_as_set]                                   # don't include the points on the path itself
     | ops.collect
    )
    


def parse_zascii_to_asg(ascii_string: str, user_specified_patterns: list = None) -> Tuple[Dict, Dict]: 
    """[summary]

    Args:
        ascii_string (str): [description]

    Returns:
        Two dictionaries
        Dict 1: The Abstract Syntax Graph (asg)
                e.g. {
                    'temp_id_0003': 
                }
        
        Dict2: A segmentation map of visited points of form {ascii_coordinate: path_group_id},
                e.g. {
                    ...
                    (11, 14): 'path_group_3',
                    (11, 15): 'path_group_3',
                    ...
                    }
        
        a list of dictionaries of elements. Some may contain graph linking, 
            but others may not (e.g. a comment will never be associated as an edge type, 
            nor a target or source of an edge)
    
    We don't need / want path groups in the final output result. These are just an intermediate step
    sample return data structure: {
        'temp_id_0000': {'type': 'ET', 'value': 'Machine', 'x': 6, 'y': 2, 'out_edges': [], 'in_edges': []},
        'temp_id_0001': {'type': 'String', 'value': 'Dog', 'x': 31, 'y': 2, 'out_edges': [], 'in_edges': []},
        'temp_id_0009': {'type': 'Edge', 'source': 'temp_id_0000', 'target': 'temp_id_0001', 'path_label': 'temp_id_0008', 'out_edges': [], 'in_edges': [], 'start_point': (2, 15), 'end_point': (2, 31), 'path': [(2, 16), (2, 17), ... (2, 30)]}
    }
    """
    # init with empty list if None. Don't pass a mutable default arg
    user_specified_patterns = user_specified_patterns or []

    s: List[str] = ascii_string.split("\n")                                             # list of single line strings    
    line_element_chars = ('|','│','-','─','┌','┐','└','┘','├','┤','┬','┴','┼','►','◄','▼','▲','>','<')
    # a list of elements ((9, 54), 'R') : candidates for characters that could be part of a text element. 
    [[_, edge_candidates],[_, expr_candidates]] = (s                                    # unpack directly from the group_by structure
        | ops.enumerate                                                                 # row index
        | ops.map[lambda p: [( (p[0], j), el) for j, el in enumerate(p[1])  ]]          # col index and form ((9, 54), '*') where * is any char
        | ops.concat                                                                   # make it a single list
        | ops.filter[lambda el: el[1] != ' ']                                           # in neither case do we consider whitespaces    
        | ops.group_by[lambda el: el[1] in line_element_chars][[True, False]]           # is this a candidate for and expression or for an edge?
    )
    elements, visited_chars = parse_all_expressions(s, expr_candidates, user_specified_patterns)    
    elements = collect_path_groups(s,  visited_chars, edge_candidates, elements)
    # return elements, visited_chars
    generate_tmp_id = make_generate_temp_uid(elements)
    edges = (list(elements.values())
     | ops.filter[lambda d: d['type']=='path_group']
     | ops.map[lambda d: split_pathgroup_into_individual_paths(d, visited_chars, s)]
     | ops.concat     
     | ops.func[lambda edge_list: {generate_tmp_id(): el for el in edge_list}]
     | ops.collect
     )
    # remove path groups and add individual paths
    elements_and_edges = { **{k:v for k,v in elements.items() if v['type']!='path_group'}, **edges}    
    # a dictionary from coordinates to a list of various elements on a given point
    seg_map = calc_multi_segmentation_map(elements_and_edges)
    for edge_id, d in list(elements_and_edges.items()) | ops.filter[lambda p: p[1]['type'] == 'Edge']:
        elements_in_halo = (calc_halo(edge_id, d, seg_map)                          # here we still have the l;ist of tuples [(5,6), (5,7),...]  of points neighboring the *unique* path (i.e. distance 1)
        | ops.map[lambda p: seg_map[p] if p in seg_map else []]         #
        | ops.concat
        | ops.func[lambda v: set(v)]                        # make ids of elements lying within halo unique    
        | ops.collect    
        )
        adjacent_elements = elements_in_halo | ops.map[lambda id: (id, elements_and_edges[id])] | ops.collect
        # Are there any elements of the wrong type next to a relation?
        for wrong_el in  adjacent_elements | ops.filter[lambda d: d[1]['type'] not in {'RT', 'Comment', 'O-Terminator', 'Edge', 'ZEFUI'}]:
            raise RuntimeError(f"The element {wrong_el[1]} was found spatially next to a relation. Only 'RT.xxxx' and comments are allowed to appear next to a relation in zascii.")
            #TODO revise this error message 

        # Is there more than one RT marking a single relation?
        adjacent_rts = adjacent_elements | ops.filter[lambda el: el[1]['type'] == 'RT'] | ops.collect
        if len(adjacent_rts) != 1:
            raise RuntimeError(f"There was not exactly RT element marking an edge. One edge had the adjacent RT elements {adjacent_rts}")
        
        # There is exactly one label for this edge. Mark this on both the label and the edge
        rt_id = adjacent_rts[0][0]
        elements_and_edges[rt_id]['labels'] = edge_id
        elements_and_edges[edge_id]['labeled_by'] = rt_id

    # We need this because visited_chars is poluted with path_group_id instead of specific edge id
    edges_path_map = {}
    for p_id, p_d  in list(elements_and_edges.items()) | ops.filter[lambda p: p[1]['type'] == 'Edge']:
        for coord in p_d['path']: edges_path_map[coord] = (p_id, p_d['labeled_by'], p_d['belongs_to']) # Tuple of (edge_id, edge_label_id, path_group_id)

    # After edges have been labeled we now assign any edge with a source or target as a O-terminator to an actual edge id
    for edge_id, d in list(elements_and_edges.items()) | ops.filter[lambda p: p[1]['type'] == 'Edge']:
        for endpoint in ['source', 'target']:
            if elements_and_edges[d[endpoint]]['type'] == 'O-Terminator':
                o_terminator = elements_and_edges[d[endpoint]]
                o_terminator_coordinate = (o_terminator['y'], o_terminator['x'])

                def find_potential_rt(cord):
                    p_id, p_labeled_by_id, p_pathgroup_id = edges_path_map[cord]
                    if p_id != edge_id and d['belongs_to'] != p_pathgroup_id: return elements_and_edges[p_labeled_by_id] # Check this path id isn't the same id as the edge the O-terminator is part of

                potentials = (
                    list([f(o_terminator_coordinate) for f in (move_left, move_right, move_up, move_down)])
                    | ops.filter[lambda cord: cord[0] < len(s) and cord[1] < len(s[cord[0]]) and s[cord[0]][cord[1]]]
                    | ops.filter[lambda cord: cord in edges_path_map]
                    | ops.map[lambda cord: find_potential_rt(cord)]    
                    | ops.filter[lambda p: p]
                    | ops.collect
                )

                if len(potentials) == 0:
                    raise RuntimeError(f"Couldn't find any edge in proximity of 1 to the O-terminator with pos: {o_terminator_coordinate}")

                if len(potentials) > 1:
                    raise RuntimeError(f"There was more than an edge in proximity of 1 to the O-terminator with pos: {o_terminator_coordinate}")

                d[endpoint] = potentials[0]['labels'] # Assign the source or target as the id of edge we are pointing to

    return elements_and_edges, visited_chars

#%%


# my_string = """


#    # template used for a zefui component
#    # default values of optional component are specified as values: the type of the AET is inferred




# """



#        RT.Something
#             ┌──────────►ET.List
#             │      o
#             │      │  RT.Age
#             │ RT.A ├────────────────────►47
#             │o<---o│  
#             │      │  RT.ListElement
#             │      └────────────────────►"hello"
#             │
#       ET.Dropdown


# """


#                                      RT.ZEFUI_OnInstantiation(z1)           # bind it to a local name
#                              ┌────────────────────────────────────►Z(z_value_inst)
#                              │
#                              │       z2:RT.ZEFUI_OnTermination(z42)
#                              ├────────────────────────────────────►Z(z_value_term)
#                              │
#                              │                            z3:RT.ZEFUI_OnValueAssignment
#                              │                         ┌───────────────────────────────►Z(z_value_val_assign)
#                              o     RT.Value(z4)        │
#                      ┌───────────────────────────────►50
#                      │
#                      │
#                      │
#                      │              RT.ZEFUI_OnInstantiation(z98)
#                      │       ┌────────────────────────────────────►Z(z_disabled_inst)
#                      │       │
#                      │       │       z6:RT.ZEFUI_OnTermination
#                      │       ├────────────────────────────────────►Z(z_disabled_term)
#                      │       │
#                      │       o     RT.Disabled?
#                      ├───────────────────────────────►False(z20)
#                      │
#                      │             RT.Min?
#     ET.IntSlider─────┼───────────────────────────────►0
#                      │
#                      │             RT.Max?
#                      ├───────────────────────────────►100(z98)
#                      │
#                      │             ZEFUI_OnInstantiation
#                      │             ZEFUI_OnTermination
#                      │             RT.Step(z10)?                 ZEFUI_OnValueAssignment:z20
#                      └──────────────────────────────────────────►10(z9)



# ========================================= bindings ============================================

# # bind local names to RAEs if they already exist. If they don't exist, it may be used in the context to 
# frame = 
# z1 = g['c063fcfbc0704fc3c03f1037d0ff4c49'] | frame
# z2 = g['7643587434704fc3c03f1037d0ff4c49'] | frame

# """


# my_rules = [
#     (
#         lambda s: s[:2] == 'Z(',                                        # predicate
#         lambda s: {'type': 'Z-Expr', 'value': s[:s.find(')')+1] }       # fct producing resulting expression
#     )
# ]
# asg, visited_chars = parse_zascii_to_asg(my_string, user_specified_patterns=my_rules)



# def make_zefui_component_template(asg: dict):
#     pass




# draw_zascii_segmentation(visited_chars)


# %%

# ascii_string1 = """
#                      RT.Normal
#                  ┌──────────►ET.List
#                  │                    ┌──────────►ET.List
#  ET.Horiz───────┐│                    │RT.ThroughV
#                 └│┐  RT.ThroughH      │
#                  │└──────────────────────────────┐        
#                  |                    │          │        
#             ET.Dropdown            ET.Vert       |        
#                                                  ▼        
#                                             ET.Hello      
# """

# test_oterminator_stuff = """
    #         RT.Normal
    #     ┌──────────►ET.List
    #     │      o
    #     │      |                   RT.Blaa
    #     │      |                ┌──────────────────►AET.String    
    #     |      |                │
    #     |      |                │  RT.Name
    #     |      |                ├──────────────────►AET.String
    #     |      |                │
    #     │      |                |
    #     |      |                |    
    #     │      │  RT.OutOfRT    o          
    #     │      └─────────────────────►ET.Dog
    #     │                      o
    #     |   RT.BetweenRTs      ▲
    #     |o───────────┬───────►o|
    #     │            |         |
    #     │            |         |
    #     │            └───────►o|
    #     │            RT.DblEnd |
    #     │                      |
    #  ET.Dropdown               |
    #                            │  RT.IntoRT              
    #                            └─────────────────────ET.Cat
# """

# ascii_string1 = """
#                           RT.Selected
#                        ┌──────────────────────────────┐
#                        │                              │
#                        │                              │
#                        │        RT.ListElement        ▼      RT.Name
#                        │     ┌─────────────────────►ET.Dog────────────────►"Rufus Alexander de Woof"
#                        │     │
#       RT.Options       │     │  RT.ListElement               RT.Name
#         ┌──────────►ET.List──┼─────────────────────►ET.Dog────────────────►761.8
#         │                    │
#         │                    │  RT.ListElement               RT.Name
#         │                    └─────────────────────►ET.Dog────────────────►(-42)
#         │
#   ET.Dropdown
# """

# Multi start and one end
# ascii_string4 = """
#        RT.Something
#        ┌───────┬──►ET.List
#        │       │
#        │       │
#        │       │RT.ListElement
#        │       │
#        │       │
#  ET.Dropdown   ET.Dog
# """


# # Edge out of edge cases to account for?
# ascii_string1 = """
#             RT.Something
#         ┌──────┬───►ET.List
#         │      o
#         │      |  RT.ListElement              
#         │      └────────────────────►ET.Dog
#         │
#   ET.Dropdown
# """

# ascii_string2 = """
#             RT.Something
#         ┌──────┬───►ET.List
#         │      o
#         |      ▲
#         │      │  RT.ListElement              
#         │      └──────────────────────ET.Dog
#         │                      
#   ET.Dropdown

# """

# ascii_string3 = """
#             RT.Something
#         ┌──────┬───►ET.List
#         │      o
#         |      ▲
#         │      │  RT.ListElement              
#         │      └───────────────┬──────ET.Dog
#         │                      o
# ET.Dropdown                    ▲
#                                │  RT.ListElement              
#                                └──────────────────────ET.Dog

# """

# # %%
# char = "─"
# prev_and_next_char_allowed_chars = {
#             "│" : ["└","┘", "─"],
#             "─": ["┌","┐","│"]
#         }[char]
# prev_and_next_char_allowed_chars












