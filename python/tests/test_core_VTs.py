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

class MyTestCase(unittest.TestCase):
    def test_pattern(self):
        d = {"a": 1,
             "b": "string"}

        self.assertIsInstance(d, Pattern[{"a": Int,
                                          "b": String}])

        self.assertIsInstance(d, Pattern[{"a": Int | String,
                                          "b": Int | String}])

        self.assertIsInstance(d, Pattern[{"a": Int | String}])
        self.assertIsInstance(d, Pattern[{"b": Any}])

        self.assertNotIsInstance(d, Pattern[{"c": Any}])
        self.assertNotIsInstance(d, Pattern[{"a": Any, "b": Any, "c": Any}])

        self.assertIsInstance(d, Pattern[{"a": Any, "b": Any, Optional["c"]: Any}])

        self.assertIsInstance(d, Pattern[{Optional["a"]: Any, Optional["b"]: Any, Optional["c"]: Any}])


if __name__ == '__main__':
    unittest.main()
