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

from urllib.request import urlopen

import sys
if len(sys.argv) >= 2:
    server = sys.argv[1]
else:
    server = "https://hub.zefhub.io"
    
try:
    response = urlopen(f"{server}/REST",
                    data=b'{"msg_type": "token", "msg_version": 1, "action": "list", "protocol_type": "ZEFDB", "protocol_version": 5}')

    b = response.read()
    import json
    j = json.loads(b)
    assert j["msg_type"] == "token_response"
    assert j["reason"] == "list"
    assert j["success"] == True

except Exception as exc:
    print(f"There was an exception when trying to get the tokens from zefhub: {exc}")

    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")
    print("NOT USING LATEST TOKENS, FALLINBACK BACK TO BOOTSTRAP!!!!")

    import shutil
    import os
    template_dir = os.path.join(os.path.dirname(__file__), "templates")
    shutil.copy(os.path.join(template_dir, "zeftypes_bootstrap_ET.json"), "zeftypes_ET.json")
    shutil.copy(os.path.join(template_dir, "zeftypes_bootstrap_RT.json"), "zeftypes_RT.json")
    shutil.copy(os.path.join(template_dir, "zeftypes_bootstrap_KW.json"), "zeftypes_KW.json")
    shutil.copy(os.path.join(template_dir, "zeftypes_bootstrap_EN.json"), "zeftypes_EN.json")

    import sys
    sys.exit(0)


et = json.dumps(j["groups"]["ET"])
rt = json.dumps(j["groups"]["RT"])
en = json.dumps(j["groups"]["EN"])
if "KW" in j["groups"]:
    kw = json.dumps(j["groups"]["KW"])
else:
    with open("zeftypes_bootstrap_KW.json") as file:
        kw = file.read()

with open("zeftypes_ET.json", "w") as file:
    file.write(et)
print("Successfully wrote ETs to zeftypes_ET.json")
with open("zeftypes_RT.json", "w") as file:
    file.write(rt)
print("Successfully wrote RTs to zeftypes_RT.json")
with open("zeftypes_EN.json", "w") as file:
    file.write(en)
print("Successfully wrote ENs to zeftypes_EN.json")
with open("zeftypes_KW.json", "w") as file:
    file.write(kw)
print("Successfully wrote KWs to zeftypes_KW.json")
