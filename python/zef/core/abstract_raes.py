from ._core import *


_custom_entity_display_names = {}

def get_custom_entity_name_dict():
    return _custom_entity_display_names


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
                'absorbed': (),
            }
        
        elif isinstance(x, Entity):
            return x

        elif isinstance(x, dict):
            assert 'type' in x and 'uid' in x
            assert type(x['type']) == EntityType
            x['absorbed'] = x.get('absorbed', ())
            self.d = x
        else:
            raise TypeError(f"can't construct an abstract entity from a {type(x)=}.  Value passed in: {x=}")

    def __repr__(self):
            # whether something is a custom entity with a overloaded repr
            # is encoded in the graph uid
            custom_entity_display_names = get_custom_entity_name_dict()
            base_name: str = (
                custom_entity_display_names[self.d['uid'].blob_uid]
                if str(self.d['uid'])[16:] == '0000000000000001'
                else f'Entity({repr(self.d["type"])}, {repr(self.d["uid"])})' 
            )
            # also show any absorbed elements
            return base_name + ''.join(('[' + repr(el) + ']' for el in self.d['absorbed']))

    def __eq__(self, other):
        if not isinstance(other, Entity): return False
        return self.d['type'] == other.d['type'] and self.d['uid'] == other.d['uid']

    def __hash__(self):
        return hash(self.d['uid'])

    def __getitem__(self, x):
        # append x to absorbed (a tuple). Return a new object
        return Entity({**self.d, 'absorbed': (*self.d['absorbed'], x)})
        


    

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
                'absorbed': (),
            }        
        elif isinstance(x, AtomicEntity):
            return x
        elif isinstance(x, dict):
            assert 'type' in x and 'uid' in x
            assert type(x['type']) == AtomicEntityType
            x['absorbed'] = x.get('absorbed', ())
            self.d = x
        else:
            raise TypeError(f"can't construct an abstract atomic entity from a {type(x)=}.  Value passed in: {x=}")

    def __repr__(self):
        return f'AtomicEntity({repr(self.d["type"])}, {repr(self.d["uid"])})' + ''.join(('[' + repr(el) + ']' for el in self.d['absorbed']))

    def __eq__(self, other):
        if not isinstance(other, AtomicEntity): return False
        return self.d['type'] == other.d['type'] and self.d['uid'] == other.d['uid']

    def __le__(self, value):
        from ._ops import assign_value
        return self | assign_value[value]
    
    def __hash__(self):
        return hash(self.d['uid'])

    def __getitem__(self, x):
        # from ._ops import merged
        # return merged[self][internal_id]
        return AtomicEntity({**self.d, 'absorbed': (*self.d['absorbed'], x)})
    

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
                'absorbed': (),
            }        
        elif isinstance(x, Relation):
            return x
        elif isinstance(x, dict):
            assert 'type' in x and 'uids' in x
            assert type(x['type']) == tuple and len(x['type']) == 3 and type(x['type'][1]) == RelationType
            assert type(x['uids']) == tuple and len(x['uids']) == 3 
            x['absorbed'] = x.get('absorbed', ())
            self.d = x
        else:
            raise TypeError(f"can't construct an abstract relation from a {type(x)=}.  Value passed in: {x=}")

    def __repr__(self):
        return f'Relation({repr(self.d["type"])}, {repr(self.d["uids"])})' + ''.join(('[' + repr(el) + ']' for el in self.d['absorbed']))

    def __eq__(self, other):
        if not isinstance(other, Relation): return False
        return self.d['type'] == other.d['type'] and self.d['uids'] == other.d['uids']

    def __hash__(self):
            return hash(''.join([str(x) for x in self.d['uids']]))
            
    def __getitem__(self, x):
        # from ._ops import merged
        # return merged[self][internal_id]
        return Relation({**self.d, 'absorbed': (*self.d['absorbed'], x)})



def abstract_rae_from_rae_type_and_uid(rae_type, uid):
    from ._ops import is_a
    if is_a(rae_type, ET):
        return Entity({"type": rae_type, "uid": uid})
    elif is_a(rae_type, AET):
        return AtomicEntity({"type": rae_type, "uid": uid})
    else:
        assert is_a(rae_type, RT)
        raise Exception("Unable to create an abstract Relation without knowing its source and target")
        







def make_custom_entity(name_to_display: str, predetermined_uid=None):
    import random
    from . import internals
    d = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

    if predetermined_uid is not None:
        if not isinstance(predetermined_uid, str):
            raise TypeError('make_custom_entity must be called with a string')
        if len(predetermined_uid) != 16:
            raise ValueError('predetermined_uid must be of length 16')
        if not all((el in d for el in predetermined_uid)):
            raise ValueError('predetermined_uid may only contain hexadecimal digits (lower case)')
    
    def generate_uid():
        return ''.join([random.choice(d) for _ in range(16)])

    src_g_uid = internals.BaseUID('0000000000000001')   # special code for custom entities
    this_uid = internals.BaseUID(generate_uid() if predetermined_uid is None else predetermined_uid)

    if name_to_display is not None:
        custom_entity_display_names: dict = get_custom_entity_name_dict()
        if this_uid in custom_entity_display_names:
            raise KeyError(f"Error in make_custom_entity: the key {this_uid} was already registered in custom_entity_display_names.")
        custom_entity_display_names[this_uid] = name_to_display

    return Entity({
        'type': ET.ZEF_CustomEntity,
        'uid': internals.EternalUID(this_uid, src_g_uid),
    })




