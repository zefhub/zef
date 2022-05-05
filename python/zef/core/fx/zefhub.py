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

from .fx_types import _Effect_Class
from ...pyzef.internals import login, logout

def zefhub_login_handler(eff: _Effect_Class):
    login()
    return {}

def zefhub_logout_handler(eff: _Effect_Class):
    logout()
    print("Warning, after logging out all graphs will have been unloaded. Any accesses of a ZefRef referencing these old graphs will cause a segmentation fault!")
    return {}