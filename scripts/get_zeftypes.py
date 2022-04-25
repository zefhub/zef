from urllib.request import urlopen
import structlog
log = structlog.get_logger()

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
    log.error("There was an exception when trying to get the tokens from zefhub", exc_info=exc)

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
log.info("Successfully wrote ETs to zeftypes_ET.json")
with open("zeftypes_RT.json", "w") as file:
    file.write(rt)
log.info("Successfully wrote RTs to zeftypes_RT.json")
with open("zeftypes_EN.json", "w") as file:
    file.write(en)
log.info("Successfully wrote ENs to zeftypes_EN.json")
with open("zeftypes_KW.json", "w") as file:
    file.write(kw)
log.info("Successfully wrote KWs to zeftypes_KW.json")
