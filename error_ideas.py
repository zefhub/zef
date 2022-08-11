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
import zef

from zef.core.op_implementations.implementation_typing_functions import ZefGenerator
from zef.core.error import _ErrorType

@func
def generate_from_x(x):
    def wrapper():
        for i in range(x):
            yield i
    return ZefGenerator(wrapper)


def inner_func(x):
    return 5/(x-16) - 1

def catching_exc(x):
    try:
        return inner_func(x)
    except Exception as exc:
        raise Exception("Nested") from exc

@func
def user_preprocess(x):
    if x > 3:
        return x + 10
    return x

@func
def outer_func(x):

    big_list = generate_from_x(x)

    slow_build_up_op = x | generate_from_x
    inner_op = user_preprocess | user_transform[user_predicate]
    slow_build_up_op = slow_build_up_op | map[inner_op]
    slow_build_up_op = slow_build_up_op | user_postprocess
    return slow_build_up_op | collect

    # return big_list | map[user_transform[user_preprocess]] | user_postprocess | collect
    #      Panic->Unexp^    ^Panic          ^Panic               ^Unexp            ^Unexp->Panic


@func
def user_transform(x, fct):
    working = 0

    try:
        while working <= x:
            if working | fct | collect: # Location of Panic exception - from frame 3 but in frame 2
                return x
            working += 1
    except _ErrorType as exc:
        raise

    return 2*x

@func
def user_predicate(x):
    pre = x*x
    def nested(x):
        return catching_exc(x)
    mid = nested(x)
    post = mid + pre

    # return post + pre > 0
    return False

@func
def user_postprocess(input):
    # some_other_stuff = thing(input)
    some_other_stuff = input

    for item in input: # Location of UnexpectedError exception - in frame 2
        pass

    return some_other_stuff

@func
def user_filter(x):
    return True




[1,2,3,10,20] | map[outer_func] | filter[user_filter] | collect # Panic exception in frame 1
    