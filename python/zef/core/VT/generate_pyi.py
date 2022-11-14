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


from zef.core import VT

value_types = [x for x in dir(VT) if isinstance(getattr(VT, x), VT.ValueType)]
value_types.sort()

import os, sys
pyi_filename = "__init__.pyi"

if len(sys.argv) == 2:
    pyi_filename = sys.argv[1]
else:
    assert len(sys.argv) == 1, "Script should be called with no arguments or one argument, the location in which to save the file."

with open(pyi_filename, "w") as file:
    file.write("""# Copyright 2022 Synchronous Technologies Pte Ltd
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

from .value_type import *

""")

    for vtype in value_types:
        file.write(f"{vtype}: ValueType_ = ...\n")
