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

from zef import *
from zef.ops import *
import time


def make_item(x):
    (ET.Machine, RT.UsedBy, x) | g | run[execute]

def find_item(x):
    from zef.pyzef.zefops import instances_impl, now as now_impl, select_by_field_impl
    # return g | now | all[ET.Machine] | filter[Z >> RT.UsedBy | value | equals[x]] | collect
    # return g | now | all[ET.Machine] | filter[lambda z: value(z >> RT.UsedBy) == x] | collect
    # return instances_impl(now_impl(g), ET.Machine) | filter[lambda z: value(z >> RT.UsedBy) == x] | collect
    # return select_by_field_impl(instances_impl(now_impl(g), ET.Machine), RT.UsedBy, x)
    # return select_by_field_impl(g | now | all[ET.Machine] | collect, RT.UsedBy, x)
    return g | now | all[ET.Machine] | select_by_field[RT.UsedBy][x] | collect

    # opts = g | now | all[ET.Machine] | filter[Z >> RT.UsedBy | value | equals[x]] | collect
    # print(len(opts))


g = Graph()
# items = [str(x) for x in range(10000)]
items = [str(x) for x in range(100)]
start = time.time()

for item in items:
    make_item(item)

making = time.time()
print(f"Time to make: {making-start}")

# import random
# random.shuffle(items)

for item in items:
    found = find_item(item)
not_found = find_item("not in there")
assert not_found is None

finding = time.time()

print(f"Time to find: {finding-making}")