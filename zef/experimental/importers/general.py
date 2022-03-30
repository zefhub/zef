
from zefdb import instantiate, AET, RT, ET, Graph, ZefRef

import networkx
from networkx.classes.digraph import DiGraph

def split_by_func(s: str, func):
    all_lists = []
    while s:
        for ind in range(len(s)):
            if func(s[ind]):
                break
        else:
            ind = ind+1
        all_lists.append(s[:ind])
        s = s[ind+1:]

    import itertools
    return list(filter(len, all_lists))
            
def default_ET_translation(name, data):
    # By default, split by anything that is punctuation or a number and turn that into words.
    words = split_by_func(name, lambda c: not c.isalpha())

    return ''.join(map(lambda w: w.capitalize(), words))

def default_RT_translation(conn, data):
    # Use the target as the RT name
    return default_ET_translation(conn[1], data)

def default_fieldname_translation(name):
    # Numbers can be important in fields, so don't lose them.
    words = split_by_func(name, lambda c: not c.isalnum())

    return ''.join(map(lambda w: w.capitalize(), words))

def default_aet_translation(name, val):
    if type(val) == bool:
        return AET.Bool
    if type(val) == int:
        return AET.Int
    if type(val) == float:
        return AET.Float
    if type(val) == str:
        return AET.String
    
    
    

def inject_networkx_into_zef(nxg: DiGraph,
                             zg: Graph,
                             ET_translation = default_ET_translation,
                             RT_translation = default_RT_translation,
                             fieldname_translation = default_fieldname_translation,
                             aet_translation = default_aet_translation,
                             ignore_excess_ET_RT_types: bool = False,
                             ignore_duplicate_fields: bool = False
                             ):
    # First check that we don't end up with a large number of ET and RT types.
    # This would be likely due to a bad conversion of the names
    if not ignore_excess_ET_RT_types:
        theset = set()
        for pair in nxg.nodes.items():
            theset.add(ET_translation(*pair))
        if len(theset) > len(nxg.nodes) / 3:
            raise Exception(f"After converting the node names, there were still {len(theset)} types left out of {len(nxg.nodes)} nodes.")

        theset = set()
        for pair in nxg.edges.items():
            theset.add(RT_translation(*pair))
        if len(theset) > len(nxg.edges) / 3:
            raise Exception(f"After converting the edge names, there were still {len(theset)} types left out of {len(nxg.edges)} edges.")


    entity_mapping = {}
    relation_mapping = {}
    for key,data in nxg.nodes.items():
        et = ET(ET_translation(key,data))

        ent = instantiate(et, zg)
        entity_mapping[key] = ent

        realise_fields(zg, ent, data, fieldname_translation, aet_translation)

    for conn,data in nxg.edges.items():
        rt = RT(RT_translation(conn, data))

        source = entity_mapping[conn[0]]
        target = entity_mapping[conn[1]]
        rel = instantiate(source, rt, target, zg)

        relation_mapping[conn] = rel

        realise_fields(zg, rel, data, fieldname_translation, aet_translation)

    return entity_mapping, relation_mapping
       
        
        
def realise_fields(zg: Graph,
                   obj: ZefRef,
                   data: dict,
                   fieldname_translation = default_fieldname_translation,
                   aet_translation = default_aet_translation,
                   ignore_duplicate_fields: bool = False):
    
    # A check - don't want accidental duplicates in the field translation, unless specifically allowed.
    # I don't think this is even possible in networkx as the fields are represented with dictionaries.
    if not ignore_duplicate_fields:
        assert(len(set(map(fieldname_translation, data.keys()))) == len(data.keys()))

    for name,val in data.items():
        rt = RT(fieldname_translation(name))
        aet = aet_translation(name, val)
        if aet is None:
            raise Exception(f"Unknown type for field with name {name} and value {val}")
        instantiate(obj, rt, instantiate(aet,zg) <= val, zg)
