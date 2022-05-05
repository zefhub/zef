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

import unittest  # pytest takes ages to run anything as soon as anything from zef is imported

from zef import *
from zef.zefops import *
import zef
import zef.tools

debug = False


class MyTestCase(unittest.TestCase):

    def test_local(self):

        # We don't want to have sync on for the real tests...
        # But sync is part of the threading issues...
        # sync = False
        sync = True
        gs = [Graph(sync) for i in range(3)]
        tags = ["first", "second", "third"]
        # gs = [Graph(sync) for i in range(1)]

        for g,g_tag in zip(gs,tags):
            tag(g, g_tag, True)

        import time, random
        from threading import Thread, current_thread

        def spam():
            global debug
            if debug:
                print(f"Starting thread: {current_thread()}")
            for i in range(100):
            # for i in range(5):
            # for i in range(1):
                if random.choice([False,True]):
                    g = random.choice(gs)
                    if debug:
                        print(f"Choosing graph directly: {g|uid}")
                else:
                    g_tag = random.choice(tags)
                    if debug:
                        print(f"Choosing graph by tag: {g_tag}")
                    g = Graph(g_tag)
                sleep_time = random.random()*0.1
                # sleep_time = 0.0
                if debug:
                    print(f"In loop {i} about to sleep for {sleep_time} with graph {g|uid}: {current_thread()}")
                with Transaction(g):
                    z = instantiate(ET.ProcessOrder, g)
                    time.sleep(sleep_time)
                    z | attach[RT.Something, 5]
                if debug:
                    print(f"End of python loop {i}: {current_thread()}")
            if debug:
                print(f"Thread finished! {current_thread()}")

                    
                
        print()
        print()
        print()
        print()
        print("Starting threads")
        print()
        print()
        # ts = [Thread(target=spam) for i in range(5)]
        # ts = [Thread(target=spam) for i in range(10)]
        ts = [Thread(target=spam) for i in range(30)]
        # ts = [Thread(target=spam) for i in range(2)]
        # ts = [Thread(target=spam) for i in range(1)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

        # Check the graphs somehow
        for g in gs:
            g | info
            for ae in g | instances[now][AET.Int]:
                # print(ae|uid, " : ", ae|value)
                x = (ae|uid, " : ", ae|value)

        # TODO: This shouldn't be needed so fix it.
        time.sleep(1)

if __name__ == '__main__':
    # debug = True
    debug = False
    unittest.main()
