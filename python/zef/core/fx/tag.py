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
from ..VT import Error

def graph_tag_handler(eff: Effect):
    try:
        from ...pyzef.main import tag
        tag(eff["graph"], eff["tag"], adding=eff["adding"], force=eff["force"])
        return {}
    except Exception as e:
        return Error(f'executing FX.Graph.Tag for effect {eff}:\n{repr(e)}')




def rae_tag_handler(eff: Effect):
    try:
        from ...pyzef.main import tag
        if not eff["adding"]:
            return Error("Untagging a RAE is not supported (yet).")
        tag(eff["rae"], eff["tag"], force_if_name_tags_other_rel_ent=eff["force"])
        return {}
    except Exception as e:
        return Error(f'executing FX.RAE.Tag for effect {eff}:\n{repr(e)}')

