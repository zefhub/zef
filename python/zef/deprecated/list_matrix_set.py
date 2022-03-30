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

from zefdb import *
from zefdb.zefops import *

##############################
# * Utils
#----------------------------

def neighbours(l):
    for i in range(1,len(l)):
        yield l[i-1],l[i]


def same_entity(x,y):
    return x|to_ezefref == y|to_ezefref
##############################
# * Sets
#----------------------------

# This is nothing special, L[RT.ListElement] is nearly identical. Just convenience functions making assumptions.

def set_toggle(s, item, rt=RT.ListElement):
    opts = (s|now) >> L[rt] | filter[lambda x: same_entity(x,item)]
    if len(opts) == 0:
        instantiate(s, rt, item, Graph(s))
    else:
        opts | only | terminate

def set_add(s: ZefRef, item: ZefRef, rt=RT.ListElement):
    opts = (s|now) >> L[rt] | filter[lambda x: same_entity(x,item)]
    if len(opts) == 0:
        instantiate(s, rt, item, Graph(s))

def set_remove(s: ZefRef, item: ZefRef, rt=RT.ListElement):
    opts = ((s|now) > L[rt]) | filter[lambda x: same_entity(x|target,item)]
    for opt in opts:
        terminate(opt)

def in_set(s, item, rt=RT.ListElement):
    opts = (s|now) >> L[rt] | filter[lambda x: same_entity(x,item)]
    return len(opts) >= 1

def set_empty(s, rt=RT.ListElement):
    for rel in (s|now) > L[rt]:
        terminate(rel)



# z1 | attach[RT.a, z2]                           # returns z1
# z_list | append_to_zef_list[z_my_element]       # returns the z_list
# z_set | add_to_zef_set[z_my_element]            # returns the z_set
# z_set | add_to_zef_set([z_my_element1, z_my_element)]            # returns the z_set
# z_list | contains[z1]


##############################
# * List
#----------------------------

# Lists have order, designated by the RT.Next relations between the relations RTListElement.

def list_start(l, rt=RT.ListElement, return_ind=False):
    opts = l > L[rt]
    if len(opts) == 0:
        if return_ind:
            return None,None
        else:
            return None
    
    start_rel = opts | first
    ind = 0
    while True:
        opts = start_rel << L[RT.Next]
        if len(opts) == 0:
            break
        if start_rel == opts | only:
            raise Exception("Invalid list state")
        start_rel = opts | only
        ind += 1

    # start = start_rel | target
    if return_ind:
        return start_rel,ind
    else:
        return start_rel

def list_end(l, rt=RT.ListElement):
    opts = l > L[rt]
    if len(opts) == 0:
        return None
    
    end_rel = opts | first
    while True:
        opts = end_rel >> L[RT.Next]
        if len(opts) == 0:
            break
        if end_rel == opts | only:
            raise Exception("Invalid list state")
        end_rel = opts | only

    # return end_rel | target
    return end_rel
        
def list_nth(l, ind, rt=RT.ListElement):
    assert ind >= 0

    start_rel,first_ind = list_start(l, rt, return_ind=True)
    if first_ind <= ind:
        search_ind = first_ind
        search_rel = (l > L[rt]) | first
    else:
        search_ind = 0
        search_rel = start_rel

    while search_ind < ind:
        search_rel = search_rel >> RT.Next
        search_ind += 1

    return search_rel

def list_as_ZefRefs(l, rt=RT.ListElement, invalid_fallback=False):
    start_rel = list_start(l, rt)
    if start_rel is None:
        return ZefRefs([], l|tx)

    cur_rel = start_rel

    out = []
    while True:
        out.append(cur_rel)
        opts = cur_rel >> L[RT.Next]
        if len(opts) == 0:
            break
        cur_rel = opts | only

    if len(out) != len(l > L[rt]):
        if invalid_fallback:
            if len(out) >= 2:
                # Note: This warning doesn't get triggered, if the "start_rel"
                # is isolated, even if other items have RT.Next on them.
                import logging
                logging.warning(f"List {l} has some RT.Next but not enough to complete the list")
            return l > L[rt]
        raise Exception(f"Warning: not all {rt} attached to {l} have {RT.Next} connecting them to make a valid list.")
    return ZefRefs(out)

def list_empty(l, rt=RT.ListElement):
    with Transaction(Graph(l)):
        for z in (l | now) > L[rt]:
            terminate(z)

def list_el_fill(l, items, rt=RT.ListElement, g=None):
    # Note: the "now" is very important, otherwise we can be adding links based on an old picture
    g = Graph(l) if g is None else g
    with Transaction(g):
        l = l | now
        list_empty(l)

        rels = []
        for item in items:
            rels.append(instantiate(l, rt, item, g))
        # can we write rels = [instantiate(l, rt, item, g) for item in items]  ?

        for prev,after in neighbours(rels):
            instantiate(prev, RT.Next, after, g)

def list_el_pushfront(l, item, rt=RT.ListElement, g=None):
    # Note: the "now" is very important, otherwise we can be adding links based on an old picture
    l = l | now
    g = Graph(l) if g is None else g
    with Transaction(g):
        start_rel = list_start(l, rt)
        rel = instantiate(l, rt, item, g)
        if start_rel is not None:
            instantiate(rel, RT.Next, start_rel, g)

def list_el_pushback(l, item, rt=RT.ListElement, g=None):
    # Note: the "now" is very important, otherwise we can be adding links based on an old picture
    l = l | now
    g = Graph(l) if g is None else g
    with Transaction(g):
        end_rel = list_end(l, rt)
        rel = instantiate(l, rt, item, g)
        if end_rel is not None:
            instantiate(end_rel, RT.Next, rel, g)

def list_pop(l, rel, g=None):
    g = Graph(l) if g is None else g
    with Transaction(g):
        l = l|now
        rel = rel|now
        following = rel >> O[RT.Next]
        preceeding = rel << O[RT.Next]
        terminate(rel)
        if following is not None and preceeding is not None:
            I(preceeding, RT.Next, following)

    

##############################
# * Matrix
#----------------------------

# Matrices have a list of rows, list of columns, and values connecting these.

def create_matrix(rows, cols, g, add_labels=True):
    I = lambda *args: instantiate(args, g)

    with Transaction(g):
        matrix = I(ET.Matrix)

        row_rels = []
        for row in rows:
            z = I(ET.Row)
            if add_labels:
                I(z, RT.Label, row)
            row_rels.append(I(matrix, RT.Row, z))

        for prev,after in neighbours(row_rels):
            I(prev, RT.Next, after)

        col_rels = []
        for col in cols:
            z = I(ET.Column)
            col_rels.append(I(matrix, RT.Column, z))
            if add_labels:
                I(z, RT.Label, col)
            
        for prev,after in neighbours(col_rels):
            I(prev, RT.Next, after)

    return matrix

def matrix_rows_as_list(matrix):
    return list_as_ZefRefs(matrix, RT.Row)
def matrix_cols_as_list(matrix):
    return list_as_ZefRefs(matrix, RT.Column)
    
def matrix_row(matrix, ind):
    return list_nth(matrix, ind, RT.Row)
def matrix_col(matrix, ind):
    return list_nth(matrix, ind, RT.Column)

def matrix_val(matrix, i, j):
    row = matrix_row(matrix, i)
    col = matrix_col(matrix, j)
    return connection(row, RT.Value, col)

def matrix_vals_as_lists(matrix):
    rows = list_as_ZefRefs(matrix, RT.Row)
    cols = list_as_ZefRefs(matrix, RT.Column)

    total = []
    for row in rows:
        this_list = []
        for col in cols:
            item = connection(row, RT.ListElement, col) >> RT.Value
            this_list.append(item)
        total.append(this_list)

    return total

def fill_matrix(matrix, items, g=None):
    # Note: the "now" is very important, otherwise we can be adding links based on an old picture
    matrix = matrix | now
    rows = matrix_rows_as_list(matrix)
    cols = matrix_cols_as_list(matrix)

    g = Graph(matrix) if g is None else g
    with Transaction(g):
        for (i,row) in enumerate(rows):
            row > L[RT.Value] | terminate
            for (j,col) in enumerate(cols):
                # connections(row, RT.Value, col) | terminate
                I(row, RT.ListElement, col) | make_fields_(RT.Value, items[i][j])

