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

from .fx_types import Effect
from ..error import Error

def graph_tag_handler(eff: Effect):
    from ...pyzef.main import tag
    tag(eff.d["graph"], eff.d["tag"], adding=eff.d["adding"], force=eff.d["force"])
    return {}


def rae_tag_handler(eff: Effect):
    from ...pyzef.main import tag
    if not eff.d["adding"]:
        return Error("Untagging a RAE is not supported (yet).")
    tag(eff.d["rae"], eff.d["tag"], force_if_name_tags_other_rel_ent=eff.d["force"])
    return {}
