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

def graph_take_transactor_role_handler(eff: Effect):
    from ...pyzef.main import make_primary
    make_primary(eff.d["graph"], True)
    return {}

def graph_release_transactor_role_handler(eff: Effect):
    from ...pyzef.main import make_primary
    make_primary(eff.d["graph"], False)
    return {}