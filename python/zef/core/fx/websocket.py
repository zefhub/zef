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

#=============================================================================== WS handlers =========================================================================================================
from .fx_types import Effect

def websocket_connect_to_server_handler(eff: Effect):
    raise NotImplementedError("websocket_connect_to_server_handler not implemented")

def websocket_start_server_handler(eff: Effect):
    raise NotImplementedError("websocket_start_server_handler not implemented")

def websocket_stop_server_handler(eff: Effect):
    raise NotImplementedError("websocket_stop_server_handler not implemented")

def websocket_send_message_handler(eff: Effect):
    raise NotImplementedError("websocket_send_message_handler not implemented")

def websocket_close_connections_handler():
    raise NotImplementedError("websocket_close_connections_handler not implemented")

