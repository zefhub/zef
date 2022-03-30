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

import unittest

from zef import *
from zef.ops import *

import asyncio


time_before_parent_sleep = None
time_after_parent_sleep = None

time_inside_client = None

async def Parent(g):
    global time_before_parent_sleep
    global time_after_parent_sleep

    with Transaction(g):
        time_before_parent_sleep = now()
        await asyncio.gather(asyncio.sleep(5), Child(g))
        time_after_parent_sleep = now()

async def Child(g):
    global time_inside_client
    await asyncio.sleep(1)
    with Transaction(g):
        time_inside_client = now()
    
class MyTestCase(unittest.TestCase):
    def test_no_trans(self):
        g = Graph()
        # TODO: Need to fix this!
        with self.assertRaises(Exception):
            asyncio.run(Parent(g))

        # This is for when things can work, or at least wait on one another.
        #self.assertTrue(time_before_parent_sleep < time_inside_client < time_after_parent_sleep)

if __name__ == '__main__':
    unittest.main()
