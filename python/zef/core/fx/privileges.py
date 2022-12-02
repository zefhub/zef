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

from .._core import *
from .._ops import *
from ...pyzef.admins import add_right, remove_right
from ..VT import *


def main_implementation(privilege, target, action, user):
    assert privilege in [KW.view, KW.append, KW.host, KW.discover, KW.modify_rights]

    s_privilege = "Allow" + to_pascal_case(str(privilege))

    if is_a(target, Graph):
        s_target = f"graph:{base_uid(target)}"
    else:
        raise NotImplementedError("TODO: handle targets other than graphs")

    assert isinstance(user, str), "Users need to be strings at the moment"
    # This is to allow groups as well as users but is only temporary while we're using strings.
    if ':' not in user:
        s_subject = "user:" + user
    else:
        s_subject = user

    ret = {}
    try:
        if action == KW.grant:
            add_right(s_subject, s_target, s_privilege)
        elif action == KW.revoke:
            remove_right(s_subject, s_target, s_privilege)
        else:
            raise Exception(f"Unknown action {action}")
        ret["action_taken"] = True

    except RuntimeError as exc:
        ret["extra_info"] = str(exc)
        if "Right not present to be removed" in str(exc):
            ret["action_taken"] = False
        elif "Right already present" in str(exc):
            ret["action_taken"] = False
        else:
            raise

    return ret

def privileges_grantview_handler(eff: dict):
    return main_implementation(KW.view, eff["target"], KW.grant, eff["user"])

def privileges_grantappend_handler(eff: dict):
    return main_implementation(KW.append, eff["target"], KW.grant, eff["user"])

def privileges_granthost_handler(eff: dict):
    return main_implementation(KW.host, eff["target"], KW.grant, eff["user"])

def privileges_grantdiscover_handler(eff: dict):
    return main_implementation(KW.discover, eff["target"], KW.grant, eff["user"])

def privileges_grantmodifyrights_handler(eff: dict):
    return main_implementation(KW.modify_rights, eff["target"], KW.grant, eff["user"])

def privileges_revokeview_handler(eff: dict):
    return main_implementation(KW.view, eff["target"], KW.revoke, eff["user"])

def privileges_revokeappend_handler(eff: dict):
    return main_implementation(KW.append, eff["target"], KW.revoke, eff["user"])

def privileges_revokehost_handler(eff: dict):
    return main_implementation(KW.host, eff["target"], KW.revoke, eff["user"])

def privileges_revokediscover_handler(eff: dict):
    return main_implementation(KW.discover, eff["target"], KW.revoke, eff["user"])

def privileges_revokemodifyrights_handler(eff: dict):
    return main_implementation(KW.modify_rights, eff["target"], KW.revoke, eff["user"])