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
from zef.ops import *
import zef

class MyTestCase(unittest.TestCase):
    def test_local_graph(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            # Creating new graph
            filepath = "file://" + os.path.join(tmpdir, "local.graph")
            g = Graph(filepath)
            created_uid = uid(g)
            z = ET.Machine | g | run

            zef.pyzef.main.save_local(g)

            # Unload graph so we can test the reload
            g = None
            import gc
            gc.collect()

            g = Graph(filepath)
            self.assertEqual(g | now | all[ET] | length | collect, 1)
            self.assertEqual(uid(g), created_uid)


if __name__ == '__main__':
    unittest.main()
