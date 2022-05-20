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

class Bytes_:
    """
    The default repr uses hexadecimal encoding.
    """
    def __init__(self, x):
        import string
        if isinstance(x, bytes):
            self.data = x
        elif isinstance(x, str) and all(c in string.hexdigits for c in x):
            self.data = x.encode()    # use utf8
        else:
            raise NotImplementedError(f"constructing a bytes from x={x}")
    
    def __repr__(self):
        return f'Bytes("{self.data.hex()}")'

    def __str__(self):
        return self.data.hex()
