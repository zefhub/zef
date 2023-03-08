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

from ..pyzef.main import *
from ..pyzef import main, zefops, internals
# from ._core import ET, AttributeEntityType, EntityType
from ..pyzef.main import EntityType
from ..pyzef.internals import ET, AttributeEntityType

main.ZefRef.__hash__ = lambda self: hash(((index(self)), index(self | zefops.tx)))
main.EZefRef.__hash__ = lambda self: index(self)

main.QuantityFloat.__hash__ = lambda self: hash((self.value, self.unit))
main.QuantityInt.__hash__ = lambda self: hash((self.value, self.unit))
main.Time.__hash__ = lambda self: hash(self.seconds_since_1970)

internals.Delegate.__hash__ = lambda self: hash((self.order, self.item))
internals.DelegateRelationTriple.__hash__ = lambda self: hash((self.rt, self.source, self.target))
internals.DelegateTX.__hash__ = lambda self: hash(internals.DelegateTX)
internals.DelegateRoot.__hash__ = lambda self: hash(internals.DelegateRoot)

# TODO: Fix this up
def delayed_instancecheck_Delegate(self, other):
    from .delegates import instancecheck_Delegate
    return instancecheck_Delegate(self, other)
internals.Delegate.__instancecheck__ = delayed_instancecheck_Delegate


from ..pyzef.main import EntityType, RelationType
from ..pyzef.internals import AttributeEntityType, EntityTypeStruct, RelationTypeStruct, AttributeEntityTypeStruct, BlobTypeStruct, BlobType
override_types = (EntityTypeStruct, RelationTypeStruct, AttributeEntityTypeStruct, BlobTypeStruct, BlobType, AttributeEntityType, EntityType, RelationType)

def or_for_types(self, other):
    from . import ValueType_
    allowed_types = (ValueType_,) + override_types
    if isinstance(other, allowed_types):
        return ValueType_(type_name='Union', absorbed=(self, other,))

    return NotImplemented
    
def and_for_types(self, other):
    from . import ValueType_
    allowed_types = (ValueType_,) + override_types
    if isinstance(other, allowed_types):
        return ValueType_(type_name='Intersection', absorbed=(self, other,))

    return NotImplemented

for x in override_types:
    x.__or__ = or_for_types
    x.__ror__ = lambda x,y: or_for_types(y,x)
    x.__and__ = and_for_types
    x.__rand__ = lambda x,y: and_for_types(y,x)


# FIXME: why do we need the standard list of timezones in the docstring for Time? Should just refer people to the TZ database. I have removed it for now.

# ------------------------- patch Time constructor to allow create of zef Time objects both from time stamps and readable strings ------------------------
from datetime import datetime, timezone, timedelta
import dateparser
import pytz   # for time zones

def create_monkey_patched_Time_ctor(old_init_fct):
    def monkey_patched_Time_ctor(self, x, timezone: str = 'Asia/Singapore'):
        """ x could be a time stamp (float/int) or a string to be parsed
        Note: the python library date parser parses dates of the following form UNINTUITIVELY, i.e. it assumes the MDY convention:  
            '9.2.2015 13:59' is read as Sep. 2. 2015
            Hence: ALWAYS stick to the convention of transforming strings to the form Time('2019-07-15 13:59', timezone='Europe/London')

            Other formats work, e.g.
            Time('9. Feb 2015 13:59')
            Time('Martes 21 de Octubre de 2014 13:23:12')    # various languages
            Time('1 เดือนตุลาคม 2005, 1:00 AM')    # Thai
            Time('1 hour ago')         # relative dates
            Time('now')

        The time zone can be specified as a string: 
            t0 = Time('2019-07-15 13:59', timezone='Europe/Berlin')
        If no time zone is specified, Singapore is assumed as a default for now - this will be cleaned up in the future.
        

        Valid timezones: see TZ database.
        """
        if(isinstance(x, float)):
            self = old_init_fct(self, x)
        elif(isinstance(x, int)):
            self = old_init_fct(self, float(x))
        elif(isinstance(x, zefops.Now)):
            self = old_init_fct(self, x)
        elif(isinstance(x, str)):
            try:
                my_timestamp = datetime.timestamp(dateparser.parse(x, settings={'TIMEZONE': timezone, 'RETURN_AS_TIMEZONE_AWARE': True}))
                self = old_init_fct(self, my_timestamp)
            except Exception:
                raise ValueError(f"Could not parse the following string as a valid datetime: \"{x}\"")
        else:
            raise ValueError('Invalid argument type passed to Time constructor')
    return monkey_patched_Time_ctor
main.Time.__init__ = create_monkey_patched_Time_ctor(Time.__init__)


def _to_date_time(self: Time, timezone:str='Asia/Singapore', str_format: str='%Y-%m-%d %H:%M:%S'):
    return datetime.fromtimestamp(self.seconds_since_1970)\
            .astimezone(pytz.timezone(timezone))\
            .strftime(str_format)

def _to_time(self: Time, timezone:str='Asia/Singapore'):
    return _to_date_time(self, timezone, str_format='%H:%M:%S')

def _to_date(self: Time, timezone:str='Asia/Singapore'):
    return _to_date_time(self, timezone, str_format='%Y-%m-%d')



main.Time.date_time = _to_date_time
main.Time.time = _to_time
main.Time.date = _to_date


def _repr_for_zef_time(self):
    return datetime.fromtimestamp(self.seconds_since_1970)\
                .astimezone(timezone(timedelta(hours=8)))\
                .strftime(f"<Time %Y-%m-%d %H:%M:%S (+0800)>")
def _str_for_zef_time(self):
    return datetime.fromtimestamp(self.seconds_since_1970)\
                .astimezone(timezone(timedelta(hours=8)))\
                .strftime(f"%Y-%m-%d %H:%M:%S (+0800)")
main.Time.__repr__ = _repr_for_zef_time
main.Time.__str__ = _str_for_zef_time


def help(arg):
    import builtins
    from . import ZefRef
    from ..zefops.base import L, value
    if isinstance(arg, ZefRef) and arg | BT == BT.ENTITY_NODE and arg | ET == ET.ZEF_Function:
        if len(arg >> L[RT.DocString]) == 1:
            print((arg >> RT.DocString) | value.String)
        else:
            print("This ZEF_Function doesn't have any DocString")
    else:
        print(builtins.help(arg))



# Temp fix fow now: these operators should be overloaded in C++ and return a new C++ type.
def _rae_get_item(self, n):
    """
    we want to write  RT.UsedBy['some internal id']. 
    Same for ETs, AETs. Monkeypatch that from Python for now.
    """
    from ._ops import instantiated
    # return {'RAE': self, 'capture': n}
    return instantiated[self][n]




main.QuantityFloat.__getitem__ = _rae_get_item
main.QuantityInt.__getitem__ = _rae_get_item



# ---------------------------- special treatment for EntityType ------------------------------------

def absorbed_get_item(self, x):
    # don't mutate, create a new instance
    from copy import copy
    new_obj = copy(self)
    if '_absorbed' not in self.__dict__:
        new_obj._absorbed = (x, )
    else:
        new_obj._absorbed = (*self._absorbed, x)
    return new_obj

def eq_with_absorbed(x, y, orig_eq):
    orig_res = orig_eq(x, y)
    if orig_res == NotImplemented:
        return NotImplemented
    
    if orig_res == False:
        return False

    try:
        absrbd1 = x.__getattribute__('_absorbed')
    except AttributeError:
        absrbd1 = ()
    
    try:
        absrbd2 = y.__getattribute__('_absorbed')
    except AttributeError:
        absrbd2 = ()

    return absrbd1 == absrbd2

def wrap_eq(typ):
    orig = typ.__eq__
    typ.__eq__ = lambda x,y,orig=orig: eq_with_absorbed(x, y, orig)
    import types
    assert type(typ.__ne__) == types.WrapperDescriptorType

def hash_with_absorbed(self, orig_hash):
    orig_res = orig_hash(self)
    try:
        absrbd = self.__getattribute__('_absorbed')
    except AttributeError:
        absrbd = ()
    return hash((orig_res,) + absrbd)

def wrap_hash(typ):
    orig = typ.__hash__
    typ.__hash__ = lambda self,orig=orig: hash_with_absorbed(self, orig)
    

def repr_with_absorbed(self, orig_repr):
    original = orig_repr(self)
    if '_absorbed' not in self.__dict__:
        return original
    else:
        return original + ''.join(('[' + repr(el) + ']' for el in self._absorbed))

def wrap_repr(typ):
    orig = typ.__repr__
    typ.__repr__ = lambda self,orig=orig: repr_with_absorbed(self, orig)
    


main.EntityType.__getitem__ = absorbed_get_item
wrap_repr(main.EntityType)
wrap_eq(main.EntityType)
wrap_hash(main.EntityType)

main.RelationType.__getitem__ = absorbed_get_item
wrap_repr(main.RelationType)
wrap_eq(main.RelationType)
wrap_hash(main.RelationType)

main.Keyword.__getitem__ = absorbed_get_item
wrap_repr(main.Keyword)
wrap_eq(main.Keyword)
wrap_hash(main.Keyword)

internals.AttributeEntityType.__getitem__ = absorbed_get_item
# wrap_repr(internals.AttributeEntityType)
wrap_eq(internals.AttributeEntityType)
wrap_hash(internals.AttributeEntityType)

internals.Delegate.__getitem__ = absorbed_get_item
wrap_repr(internals.Delegate)
wrap_eq(internals.Delegate)
wrap_hash(internals.Delegate)






# we want to allow for e.g. "AET.Float <= 42.1"
# return a ZefOp to use as a container here.
# We used a dict previously, but then we could not 
# catch the case "AET.Float['abc'] <= 42.1"
# For consistency, we use Lazy ZefOps as intermediate containers
def leq_monkey_patching_ae(self, other):
    from ._ops import assign, LazyValue
    return LazyValue(self) | assign[other]
    
AttributeEntityType.__le__ = leq_monkey_patching_ae


# # Pretty printing for ZefRefs
# def pprint_ZefRefs(self, p, cycle):
#     p.text(str(self))
#     p.text(" [\n")

#     N_max = 10
#     if len(self) > N_max:
#         # Split into first part and last part
#         N = N_max // 2
#         first = [self[i] for i in range(N)]
#         last = [self[i] for i in range(len(self)-N, len(self)-1)]
#         p.text('\n'.join("\t"+str(x) for x in first))
#         p.text('\n\t...\n')
#         p.text('\n'.join("\t"+str(x) for x in last))
#     else:
#         p.text('\n'.join("\t"+str(x) for x in self))
#     p.text(']')

# main.ZefRefs._repr_pretty_ = pprint_ZefRefs
# main.EZefRefs._repr_pretty_ = pprint_ZefRefs

def convert_to_assign(self, value):
    from ._ops import assign
    return self | assign[value]
ZefRef.__le__ = convert_to_assign
EZefRef.__le__ = convert_to_assign

original_Graph__contains__ = main.Graph.__contains__
def Graph__contains__(self, x):
    from .abstract_raes import EntityRef_, AttributeEntityRef_, RelationRef_
    from ._ops import origin_uid, to_delegate
    from .internals import val_as_serialized_if_necessary
    from .atom import Atom_
    if type(x) in [EntityRef_, AttributeEntityRef_, RelationRef_, Atom_]:
        return origin_uid(x) in self

    from .VT import Delegate
    if isinstance(x, Delegate):
        # In case x is a BlobPtr, convert it to DelegateRef first
        d = to_delegate(x)
        maybe_z = to_delegate(d, self)
        return maybe_z is not None

    from .VT import Val
    if isinstance(x, Val):
        val = val_as_serialized_if_necessary(x)
        maybe_z = self.get_value_node(val)
        return maybe_z is not None

    if type(x) in [ZefRef, EZefRef]:
        if Graph(x) == self:
            return True
        return origin_uid(x) in self

    return original_Graph__contains__(self, x)
main.Graph.__contains__ = Graph__contains__
    
original_Graph__getitem__ = main.Graph.__getitem__
def Graph__getitem__(self, x):
    from .abstract_raes import EntityRef_, AttributeEntityRef_, RelationRef_
    from .atom import Atom_
    from ._ops import uid, target, to_delegate, origin_uid
    from .internals import BT, val_as_serialized_if_necessary
    if type(x) in [EntityRef_, AttributeEntityRef_, RelationRef_, Atom_]:
        return self[origin_uid(x)]

    from .VT import Delegate
    if isinstance(x, Delegate):
        # In case x is a BlobPtr, convert it to DelegateRef first
        d = to_delegate(x)
        maybe_z = to_delegate(d, self)
        if maybe_z is None:
            raise KeyError(f"Delegate {x} not present in graph")
        return maybe_z

    from .VT import Val
    if isinstance(x, Val):
        val = val_as_serialized_if_necessary(x)
        maybe_z = self.get_value_node(val)
        if maybe_z is None:
            raise KeyError(f"ValueNode {x} doesn't exist on graph") 
        return maybe_z
        

    res = original_Graph__getitem__(self, x)
    # We magically transform any FOREIGN_ENTITY_NODE accesses to the real RAEs.
    # Accessing the low-level BTs can only be done through traversals
    if BT(res) in [BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE]:
        # return target(res << BT.ORIGIN_RAE_EDGE)
        # TODO: We need a consistent way of accessing this.
        #
        # Is this even possible for a EZefRef? User can terminate and recreate
        # RAEs that have the same identity, so there's nothing sensible that
        # could be returned here?
        #
        # Hence, going to disable this. It will be that you can only get the RAE
        # corresponding to a foreign RAE in the context of a graph slice. Doing
        # it here will just return the low level blob.
        pass
    return res

main.Graph.__getitem__ = Graph__getitem__

original_Graph__init__ = main.Graph.__init__
def Graph__init__(self, *args, **kwds):
    if len(kwds) == 0 and len(args) == 1:
        from .graph_slice import GraphSlice_
        from .atom import AtomClass, _get_ref_pointer
        if isinstance(args[0], GraphSlice_):
            return original_Graph__init__(self, args[0].tx)
        elif isinstance(args[0], AtomClass):
            return original_Graph__init__(self, _get_ref_pointer(args[0]))

    return original_Graph__init__(self, *args, **kwds)
main.Graph.__init__ = Graph__init__


from ..pyzef.zefops import SerializedValue

def SerializedValue_deserialize(self):
    if self.type == "tools.serialize":
        from .serialization import deserialize
        from json import loads
        return deserialize(loads(self.data))
    else:
        print(self.type)
        print(self.data)
        raise Exception(f"Don't know how to deserialize a type of {self.type}")
SerializedValue.deserialize = SerializedValue_deserialize

def SerializedValue_serialize(value):
    from .serialization import serialize, serialization_mapping, is_python_scalar_type
    from json import dumps
    if type(value) in serialization_mapping or is_python_scalar_type(value):
        return SerializedValue("tools.serialize", dumps(serialize(value)))
    else:
        raise Exception(f"Don't know how to serialize a type of {value}")
SerializedValue.serialize = SerializedValue_serialize

def SerializedValue_repr(self):
    return f"SerializedValue('{self.type}', '{self.data}')"
SerializedValue.__repr__ = SerializedValue_repr



def AttributeEntityTypeStruct_getitem(self, x):
    return AttributeEntityType(SerializedValue.serialize(x))
AttributeEntityTypeStruct.__getitem__ = AttributeEntityTypeStruct_getitem

original_AttributeEntityType__repr__ = internals.AttributeEntityType.__repr__
def AttributeEntityType_repr(self):
    s = "AET"
    if self.complex_value:
        s += "(" + str(self.complex_value.deserialize()) + ")"
    else:
        s += "." + str(self.rep_type)
    if '_absorbed' in self.__dict__:
        s += ''.join(('[' + repr(el) + ']' for el in self._absorbed))
    return s
AttributeEntityType.__repr__ = AttributeEntityType_repr

original_AttributeEntityType__str__ = internals.AttributeEntityType.__str__
def AttributeEntityType_str(self):
    if self.complex_value:
        return "COMPLEX(" + str(self.complex_value.deserialize()) + ")"
    else:
        return str(self.rep_type)
AttributeEntityType.__str__ = AttributeEntityType_str



#                           _____         _    _  _               ___   _        _              _       _   _         _           _    _                                       
#                          | ____| _ __  | |_ (_)| |_  _   _     / _ \ | |__    (_)  ___   ___ | |_    | \ | |  ___  | |_   __ _ | |_ (_)  ___   _ __                          
#   _____  _____  _____    |  _|  | '_ \ | __|| || __|| | | |   | | | || '_ \   | | / _ \ / __|| __|   |  \| | / _ \ | __| / _` || __|| | / _ \ | '_ \     _____  _____  _____ 
#  |_____||_____||_____|   | |___ | | | || |_ | || |_ | |_| |   | |_| || |_) |  | ||  __/| (__ | |_    | |\  || (_) || |_ | (_| || |_ | || (_) || | | |   |_____||_____||_____|
#                          |_____||_| |_| \__||_| \__| \__, |    \___/ |_.__/  _/ | \___| \___| \__|   |_| \_| \___/  \__| \__,_| \__||_| \___/ |_| |_|                        
#                                                      |___/                  |__/                                                                                             


class EntityValueInstance_:
    def __init__(self, arg, *args, **kwargs):
        from .VT import ET, Entity
        if isinstance(arg, ET):
            self._entity_type = arg
        elif isinstance(arg, Entity):
            self._entity_type = ET(arg)
            args = (origin_uid(arg),) + args
        else:
            raise Exception(f"Don't understand arg type: {arg}")
        self._args = args
        self._kwargs = kwargs
        
    def __repr__(self):
        nl = '\n'
        items = [str(arg) for arg in self._args]
        items += [f"{k}={v!r}" for k,v in self._kwargs.items()]
        return f'{self._entity_type}({f", ".join(items)})'
    
    def __getattr__(self, name):
        # return self._kwargs[name]
        from ._ops import F
        return self | getattr(F, name)
    
    def __eq__(self, other):
        if not isinstance(other, EntityValueInstance_): return False
        return self._entity_type == other._entity_type and self._kwargs == other._kwargs

    def __getitem__(self, name):
        new_et = self._entity_type[name]
        return EntityValueInstance_(new_et, *self._args, **self._kwargs)

    def __call__(self, *args, **kwargs):
        new_kwargs = dict(self._kwargs)
        new_kwargs.update(kwargs)
        return EntityValueInstance_(self._entity_type, *(self._args + args), **new_kwargs)

    def __hash__(self):
        from .VT.value_type import hash_frozen
        return hash_frozen(("EntityValueInstance", self._entity_type, self._args, self._kwargs))
    
    def clone(self):
        return EntityValueInstance_(self._entity_type, *self._args, **{k: v.clone() if isinstance(v, EntityValueInstance_) else v for k,v in self._kwargs.items()})




def entity_type_call_func(self, *args, **kwargs):
    return EntityValueInstance_(EntityType(self.value), **kwargs)

EntityType.__call__ = entity_type_call_func
