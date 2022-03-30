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

import os, sys

assert len(sys.argv) == 2, "Need one argument: mode ('early' | 'created')"

mode = sys.argv[1]

if mode == "early":
    filename = "early.tokens"
elif mode == "created":
    filename = "all_created.tokens"
else:
    raise Exception("Mode should be early or created")

import time
# for pulled_filename in ["../zeftypes_ET.json", "../zeftypes_RT.json", "../zeftypes_EN.json"]:
#     if time.time() - os.path.getmtime(pulled_filename) > 600:
#         print(f"The modification time of {pulled_filename} is more than 10min ago, please rerun get_zeftypes.py and recompile.")
#         sys.exit(1)

if time.time() - os.path.getmtime(filename) > 600:
    print(f"The modification time of {filename} is more than 10min ago, please run create_tokens_files.py to refresh these files.")
    sys.exit(1)
with open(filename) as file:
    l_raw = file.read().split('\n')

# Remove any empty lines just in case, and also strip
l = []
for x in l_raw:
    x = x.strip()
    if x == "":
        continue
    l += [x]

if not l:
    print("No tokens to add!")
    sys.exit(0)


print("About to add these tokens:")
for x in l:
    print(f"'{x}'")

print("Type 'yes' if you would like to continue")
check = input()

if check != "yes":
    sys.exit(1)

from zef.pyzef.admins import token_management
for x in l:
    group,name = x.split('.',1)
    token_management("add", group, name, "group:everyone")
    print(f"Did add of {group=} and {name=}")
