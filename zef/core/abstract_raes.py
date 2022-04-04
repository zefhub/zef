from ._core import *

#                                     _     _           _                       _       ____      _     _____      ____  _                                                            
#                                    / \   | |__   ___ | |_  _ __   __ _   ___ | |_    |  _ \    / \   | ____|    / ___|| |  __ _  ___  ___   ___  ___                                
#   _____  _____  _____  _____      / _ \  | '_ \ / __|| __|| '__| / _` | / __|| __|   | |_) |  / _ \  |  _|     | |    | | / _` |/ __|/ __| / _ \/ __|    _____  _____  _____  _____ 
#  |_____||_____||_____||_____|    / ___ \ | |_) |\__ \| |_ | |   | (_| || (__ | |_    |  _ <  / ___ \ | |___    | |___ | || (_| |\__ \\__ \|  __/\__ \   |_____||_____||_____||_____|
#                                 /_/   \_\|_.__/ |___/ \__||_|    \__,_| \___| \__|   |_| \_\/_/   \_\|_____|    \____||_| \__,_||___/|___/ \___||___/                               
#                                                                                                                                                                                     


class Entity:
    """ 
    A value representation of an "abstract entity" (in the 
    sense of an abstract vs concrete object 
    https://plato.stanford.edu/entries/abstract-objects/).

    This object/value can be used to refer to an entity
    without the context of a reference frame (graph slice: 
    subject and subject's time) or a timeless observational
    frame (graph). An abstract entity itself is timeless.

    It is simply a wrapper around the entity's type and its
    origin's id (serving as an identifier for its lineage group.)

    This will become a zef value in future.
    """
    def __init__(self, x):
        from ._ops import origin_uid
        if isinstance(x, ZefRef) or isinstance(x, EZefRef):
            assert BT(x)==BT.ENTITY_NODE
            self.d = {
                'type': ET(x),
                'uid': origin_uid(x),
            }
        
        elif isinstance(x, Entity):
            return x

        elif isinstance(x, dict):
            assert 'type' in x and 'uid' in x
            assert type(x['type']) == EntityType
            self.d = x
        else:
            raise TypeError(f"can't construct an abstract entity from a {type(x)=}.  Value passed in: {x=}")

    def __repr__(self):
        return f'Entity({repr(self.d["type"])}, {repr(self.d["uid"])})'

    def __eq__(self, other):
        if not isinstance(other, Entity): return False
        return self.d['type'] == other.d['type'] and self.d['uid'] == other.d['uid']

    def __hash__(self):
        return hash(self.d['uid'])

    def __getitem__(self, internal_id):
        from ._ops import merged
        return merged[self][internal_id]

    

class AtomicEntity:
    """ 
    A value representation of an "abstract atomic entity".
    This will become a zef value in future.
    """
    def __init__(self, x):
        from ._ops import origin_uid
        if isinstance(x, ZefRef) or isinstance(x, EZefRef):
            assert BT(x)==BT.ATOMIC_ENTITY_NODE
            self.d = {
                'type': AET(x),
                'uid': origin_uid(x),
            }        
        elif isinstance(x, AtomicEntity):
            return x
        elif isinstance(x, dict):
            assert 'type' in x and 'uid' in x
            assert type(x['type']) == AtomicEntityType
            self.d = x
        else:
            raise TypeError(f"can't construct an abstract atomic entity from a {type(x)=}.  Value passed in: {x=}")

    def __repr__(self):
        return f'AtomicEntity({repr(self.d["type"])}, {repr(self.d["uid"])})'

    def __eq__(self, other):
        if not isinstance(other, AtomicEntity): return False
        return self.d['type'] == other.d['type'] and self.d['uid'] == other.d['uid']

    def __le__(self, value):
        from ._ops import assign_value
        return self | assign_value[value]
    
    def __hash__(self):
        return hash(self.d['uid'])

    def __getitem__(self, internal_id):
        from ._ops import merged
        return merged[self][internal_id]
    

class Relation:
    """ 
    A value representation of an "abstract relation".
    This is a thin wrapper around the type (triple)

    It will become a zef value in future.
    """
    def __init__(self, x):
        from ._ops import origin_uid, rae_type, source, target
        if isinstance(x, ZefRef) or isinstance(x, EZefRef):
            assert BT(x)==BT.RELATION_EDGE
            self.d = {
                'type': (rae_type(source(x)), RT(x), rae_type(target(x))),
                'uids': (origin_uid(source(x)), origin_uid(x), origin_uid(target(x))),
            }        
        elif isinstance(x, Relation):
            return x
        elif isinstance(x, dict):
            assert 'type' in x and 'uids' in x
            assert type(x['type']) == tuple and len(x['type']) == 3 and type(x['type'][1]) == RelationType
            assert type(x['uids']) == tuple and len(x['uids']) == 3 
            self.d = x
        else:
            raise TypeError(f"can't construct an abstract relation from a {type(x)=}.  Value passed in: {x=}")

    def __repr__(self):
        return f'Relation({repr(self.d["type"])}, {repr(self.d["uids"])})'

    def __eq__(self, other):
        if not isinstance(other, Relation): return False
        return self.d['type'] == other.d['type'] and self.d['uids'] == other.d['uids']

    def __hash__(self):
            return hash(''.join([str(x) for x in self.d['uids']]))
            
    def __getitem__(self, internal_id):
        from ._ops import merged
        return merged[self][internal_id]


def abstract_rae_from_rae_type_and_uid(rae_type, uid):
    from ._ops import is_a
    if is_a(rae_type, ET):
        return Entity({"type": rae_type, "uid": uid})
    elif is_a(rae_type, AET):
        return AtomicEntity({"type": rae_type, "uid": uid})
    else:
        assert is_a(rae_type, RT)
        raise Exception("Unable to create an abstract Relation without knowing its source and target")
        
