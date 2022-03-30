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

class Bytes:
    def __init__(self, data):
        import string
        if isinstance(data, bytes):
            self.data = data
            self.is_hex = False
        elif isinstance(data, str) and all(c in string.hexdigits for c in data):
            self.data = data
            self.is_hex = True
        else:
            raise NotImplementedError
    
    def __repr__(self):
        if self.is_hex: return f'Bytes("{self.data})"'
        else: return f'Bytes("{self.data.hex()}")'

    def __str__(self):
        if self.is_hex: return self.data
        else: return self.data.hex()