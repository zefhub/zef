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

"""
This is an expression type / value that most users do not have to interact with directly.

There are two main use cases for it at the moment:

A)  It allows for a more readable syntax when writing queries.
    The syntax is similar to that in Pandas and R's Deplyr
    .
    OldPerson = ET.Person & (z.Age > 60)
    
    g = Graph()
    [{
        ET.Movie: {
            RT.Title: 'Pulp Fiction',
            RT.Director: { ET.Person: { RT.FirstName: 'Quentin', RT.Age: 60 } },
        },    
    },
    {ET.Person: { RT.FirstName: 'Fred', RT.Age: 30 }},
    ] | g | run
    g | now | all[OldPerson] | c

    Why can we not just use SymbolicExpressions for this? We want `(z.Age > 60)` to 
    immediate evaluate to a ValueType. If this were not the case, this would break
    the type system / add massive complications. Therefore this new type is created.

B)  We could also use this type as variables in queries, to which values are 
    bound within some context.
"""

from .VT import  Is
from ._ops import equals, greater_than, less_than, greater_than_or_equal, less_than_or_equal, collect, F, alias

class ZField_:
    """
    make this a special type: we want to use it in expressions like
    (z.age > 10)   # and this should evaluate to a ValueType.

    This is also used in structural pattern matching.
    """
    def __init__(self, name):
        self.name = name
    
    def __repr__(self):
        return f'z.{self.name}'

    def __eq__(self, other):
        return (Is[F.__getattr__(self.name) | equals[other] ] 
        | alias[f"(z.{self.name} == {other})"] 
        | collect
        )

    def __gt__(self, other):
        return (Is[F.__getattr__(self.name) | greater_than[other] ] 
        | alias[f"(z.{self.name} > {other})"] 
        | collect
        )

    def __ge__(self, other):
        return (Is[F.__getattr__(self.name) | greater_than_or_equal[other] ] 
        | alias[f"(z.{self.name} >= {other})"] 
        | collect
        )

    def __lt__(self, other):
        return (Is[F.__getattr__(self.name) | less_than[other] ] 
        | alias[f"(z.{self.name} < {other})"] 
        | collect
        )

    def __le__(self, other):
        return (Is[F.__getattr__(self.name) | less_than_or_equal[other] ] 
        | alias[f"(z.{self.name} <= {other})"] 
        | collect
        )


