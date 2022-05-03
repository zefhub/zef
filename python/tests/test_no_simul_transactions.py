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
