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

from .. import report_import
report_import("zef.core.user_value_type")

#                           _   _                  __     __        _               _____                                             
#                          | | | | ___   ___  _ __ \ \   / /  __ _ | | _   _   ___ |_   _| _   _  _ __    ___                         
#   _____  _____  _____    | | | |/ __| / _ \| '__| \ \ / /  / _` || || | | | / _ \  | |  | | | || '_ \  / _ \    _____  _____  _____ 
#  |_____||_____||_____|   | |_| |\__ \|  __/| |     \ V /  | (_| || || |_| ||  __/  | |  | |_| || |_) ||  __/   |_____||_____||_____|
#                           \___/ |___/ \___||_|      \_/    \__,_||_| \__,_| \___|  |_|   \__, || .__/  \___|                        
#                                                                                          |___/ |_|                                  

from .VT import String, ValueType, Dict, Any, make_VT, is_type_name_, PyDict

_user_value_type_registry = {}    # append to this: keep track of all types known to this runtime

# class UserValueType_:
#     def __init__(self, name: String, representation_type, constraints):
#         from random import randint
#         self.name = name
#         self.representation_type = representation_type
#         self.constraints = constraints
#         self.user_type_id = str(randint(0, 100000000000))     # TODO: use a Ref: the type is originally created on the respective process graph

#         # add to registry
#         _user_value_type_registry[self.user_type_id] = self

#     def __repr__(self):
#         return f"UserValueType(name={self.name}, representation_type={self.representation_type}, constraints={self.constraints})"

#     def __call__(self, *args, **kwargs):
#         """
#         equivalent to the constructor for this custom type
#         """
        
#         if bool(kwargs):
#             # if keyword args, then the representation_type MUST be a dict and no positional args are allowed
#             assert args == ()
#             assert self.representation_type in {Dict, dict}
#             val = kwargs

#         else:
#             # if positional args, then the representation_type MUST be a tuple and no keyword args are allowed
#             assert bool(kwargs)==False    # no keyword args
#             if len(args) == 1:         # if there is only one positional arg, then it is the value itself
#                 val = args[0]
#             elif len(args) == 0 and self.representation_type in {Dict, dict}:
#                 # The constructor is valid without args only in the case of a dictionary
#                 val = {}
#             else:
#                 raise ValueError("Error initializing UserValueType")

#         try:
#             cast_val = self.representation_type(val)
#         except:
#             raise Exception(f"UserValueType(name={self.name}) cannot cast '{val}' to {self.representation_type}")

#         # check whether the constraints are satisfied
#         if not is_a(cast_val, self.constraints):
#             raise Exception(f"UserValueType(name={self.name}) constraint does not match")
        
#         return UserValueInstance_(self.user_type_id, cast_val)
        
#     def __eq__(self, other):
#         if type(other) is not type(self):
#             return False
#         if other.user_type_id != self.user_type_id:
#             return False
#         return True

#     # TODO: just change this to a ValueType directly instead of this hack
#     def __instancecheck__(self, other):
#         if not isinstance(other, UserValueInstance_):
#             return False
#         return other.user_type_id == self.user_type_id
#def UVT_ctor(self, name: String, representation_type, constraints):
def UVT_ctor(self, *args, **kwargs):
    if "user_type_id" in self._d:
        # if bool(kwargs):
        #     # if keyword args, then the representation_type MUST be a dict and no positional args are allowed
        #     assert args == ()
        #     assert self.representation_type in {Dict, dict}
        #     val = kwargs

        # else:
        #     # if positional args, then the representation_type MUST be a tuple and no keyword args are allowed
        #     assert bool(kwargs)==False    # no keyword args
        #     if len(args) == 1:         # if there is only one positional arg, then it is the value itself
        #         val = args[0]
        #     elif len(args) == 0 and self.representation_type in {Dict, dict}:
        #         # The constructor is valid without args only in the case of a dictionary
        #         val = {}
        #     else:
        #         raise ValueError("Error initializing UserValueType")
        # try:
        #     cast_val = self.representation_type(val)
        # except:
        #     raise Exception(f"UserValueType(name={self.name}) cannot cast '{val}' to {self.representation_type}")

        try:
            cast_val = self._d["representation_type"](*args, **kwargs)
        except:
            raise ValueError("Couldn't construct type")

        # check whether the constraints are satisfied
        if not isinstance(cast_val, self._d["constraints"]):
            raise Exception(f"UserValueType(name={self._d['name']}) constraint ({self._d['constraints']}) does not match")
        
        return UserValueInstance(self._d["user_type_id"], cast_val)
    else:
        name, representation_type, constraints = args
        allowed_keys = {"forced_uid", "object_methods"}
        assert all(x in allowed_keys for x in kwargs)
        the_uid = kwargs.get("forced_uid", None)
        if the_uid is None:
            from random import randint
            the_uid = str(randint(0, 100000000000))
        object_methods = kwargs.get("object_methods", {})
        new_uvt = self._replace(
            name=name,
            representation_type=representation_type,
            constraints=constraints,
            user_type_id=the_uid,     # TODO: use a Ref: the type is originally created on the respective process graph
            object_methods=object_methods,
        )

        # add to registry
        assert new_uvt._d["user_type_id"] not in _user_value_type_registry
        _user_value_type_registry[new_uvt._d["user_type_id"]] = new_uvt
        return new_uvt

def UVT_str(self):
    if "name" in self._d:
        return f"UserValueType(name={self._d['name']}, representation_type={self._d['representation_type']}, constraints={self._d['constraints']})"
    else:
        return "UserValueType"

        
def UVT_is_a(x, typ):
    if "user_type_id" in typ._d:
        if not isinstance(x, UserValueInstance):
            return False
        return x._user_type_id == typ._d["user_type_id"]
    else:
        if isinstance(x, UserValueInstance):
            # Or should this be false?
            return True
        if is_type_name_(x, "UserValueType"):
            return True
        return False

UserValueType = make_VT('UserValueType',
                        constructor_func=UVT_ctor,
                        pass_self=True,
                        str_func=UVT_str,
                        is_a_func=UVT_is_a)

class UserValueInstance_:
    def __init__(self, user_type_id, value):
        # only store the id here, not the name?
        self._user_type_id = user_type_id
        self._value = value

    def _get_type(self):
        return _user_value_type_registry[self._user_type_id]
        
    def __repr__(self):
        # look up the name in the _user_value_type_registry from the id
        type_name = self._get_type()._d["name"]
        return f"{type_name}({repr(self._value)})"

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        if other._user_type_id != self._user_type_id:
            return False
        if other._value != self._value:
            return False
        return True

    def __hash__(self):
        from .VT.value_type import hash_frozen
        return hash_frozen((self._user_type_id, self._value))
    

    def __getattr__(self, other):
        if other.startswith("_"):
            return object.__getattribute__(self, other)
        typ = self._get_type()
        getattr_func = typ._d["object_methods"].get("getattr", None)
        if getattr_func is not None:
            return getattr_func(self, other)
        if isinstance(self._value, PyDict):
            return self._value[other]
        else:
            return getattr(self._value, other)

    def __dir__(self):
        if isinstance(self._value, PyDict):
            return list(self._value.keys())
        return dir(self._value)

    # required to enable dict(my_user_value_type_instance)
    # fingers crossed that nobody has a field 'keys' 
    # and tries to do my_user_value_type_instance.keys
    def _keys(self):
        if not isinstance(self._value, PyDict):
            raise AttributeError("UserValueInstance 'keys' only works on dicts")
        return self._value.keys()

    def __getitem__(self, key):
        if not isinstance(self._value, PyDict):
            raise AttributeError("UserValueInstance 'getitem' only works on dicts")
        return self._value[key]

    def __contains__(self, key):
        if not isinstance(self._value, PyDict):
            raise AttributeError("UserValueInstance 'contains' only works on dicts")
        return key in self._value

UserValueInstance = make_VT('UserValueInstance', pytype=UserValueInstance_)