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


#                           _   _                  __     __        _               _____                                             
#                          | | | | ___   ___  _ __ \ \   / /  __ _ | | _   _   ___ |_   _| _   _  _ __    ___                         
#   _____  _____  _____    | | | |/ __| / _ \| '__| \ \ / /  / _` || || | | | / _ \  | |  | | | || '_ \  / _ \    _____  _____  _____ 
#  |_____||_____||_____|   | |_| |\__ \|  __/| |     \ V /  | (_| || || |_| ||  __/  | |  | |_| || |_) ||  __/   |_____||_____||_____|
#                           \___/ |___/ \___||_|      \_/    \__,_||_| \__,_| \___|  |_|   \__, || .__/  \___|                        
#                                                                                          |___/ |_|                                  

from .VT import String, ValueType
from ._ops import is_a


_user_value_type_registry = {}    # append to this: keep track of all types known to this runtime

class UserValueType_:
    def __init__(self, name: String, representation_type, constraints):
        from random import randint
        self.name = name
        self.representation_type = representation_type
        self.constraints = constraints
        self.user_type_id = str(randint(0, 100000000000))     # TODO: use a Ref: the type is originally created on the respective process graph

        # add to registry
        _user_value_type_registry[self.user_type_id] = self

    def __repr__(self):
        return f"UserValueType(name={self.name}, representation_type={self.representation_type}, constraints={self.constraints})"

    def __call__(self, val):
        """
        equivalent to the constructor for this custom type
        """

        try:
            cast_val = self.representation_type(val)
        except:
            raise Exception(f"UserValueType(name={self.name}) cannot cast '{val}' to {self.representation_type}")

        # check whether the constraints are satisfied
        if not is_a(cast_val, self.constraints):
            raise Exception(f"UserValueType(name={self.name}) constraint does not match")
        
        return UserValueInstance_(self.user_type_id, cast_val)
        
    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        if other.user_type_id != self.user_type_id:
            return False
        return True


# use a constructor function distinct from the type
def UserValueType(name: String, representation_type: ValueType, constraints: ValueType=None):
    return UserValueType_(name, representation_type, constraints)



class UserValueInstance_:
    def __init__(self, user_type_id, value):
        # only store the id here, not the name?
        self.user_type_id = user_type_id
        self.value = value
        
    def __repr__(self):
        # look up the name in the _user_value_type_registry from the id
        type_name = _user_value_type_registry[self.user_type_id].name
        return f"{type_name}({repr(self.value)})"

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        if other.user_type_id != self.user_type_id:
            return False
        if other.value != self.value:
            return False
        return True