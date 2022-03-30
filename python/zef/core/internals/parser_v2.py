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

from ctypes import *


def convert_from_v1(blobs):
    out = {}

    for index,item in blobs.items():
        new_item = {**item}
        new_item["_old_index"] = index
        if "uid" in item:
            new_item["_internalUID"] = item["uid"]
        else:
            new_item["_internalUID"] = index

        out[index] = new_item

    return out
