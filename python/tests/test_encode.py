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
    def test_encode(self):
        # A 64bit unsigned
        d = {"hash": 13625394757606569013, "negative": -5123, "list": [1,2,{"nested": "dict"}]}

        from zef.pyzef.internals import prepare_ZH_message, parse_ZH_message
        data = (d, [b"123", b"456"])
        self.assertEqual(data, parse_ZH_message(prepare_ZH_message(*data)))

        data = ({"tuples": (1,2,3)}, [])
        # Because of the tuple, should error
        with self.assertRaises(Exception):
            prepare_ZH_message(*data)
        import json
        ret_data = ({"tuples": [1,2,3]}, [])
        self.assertEqual(ret_data, parse_ZH_message(prepare_ZH_message(json.dumps(data[0]), data[1])))
                


if __name__ == '__main__':
    unittest.main()
