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
Z expressions are useful notation for succinctly declaring sets of things.
e.g. (Z > 10) denotes the set of all thigns greater than 10.

This can be chained though: zef functions and all kinds of operators can be absorbed.
e.g. IsJohn = (Z.first_name | to_uppercase = 'JOHN')

Z expressions sit right at the top of the food chain when it comes to operator binding.
A Z-expression gobbles up anything to become a new Z-Expression.

So how can we use them to declare sets of things? As soon as a Z expression encounters
a binary comparison operator ('=', '>', '<', '>=', '<=', '!=') it will stop gobbling
up operators and instead become a ValueType.
"""


class ZExpression_:
    pass


ZZ = ZExpression_()
