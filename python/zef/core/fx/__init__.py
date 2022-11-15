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

from ... import report_import
report_import("zef.core.fx")

from .fx_types import FX, Effect

from .state import _state


# from .http import (
#     http_start_server_handler,
#     http_stop_server_handler,
#     http_send_response_handler,
# )
from .http import (
    http_start_server_handler,
    http_stop_server_handler,
    http_send_response_handler,
    http_send_request_handler
)


from .websocket import (
    websocket_connect_to_server_handler,
    websocket_start_server_handler, 
    websocket_stop_server_handler, 
    websocket_send_message_handler, 
    websocket_close_connections_handler,
    )


from .graphql import (
    graphql_start_server_handler,
    graphql_stop_server_handler,
    graphql_start_playground_handler,
    graphql_stop_playground_handler,
    graphql_generate_schema_string_handler,
)


from .stream import (
    stream_create_pushable_stream_handler,
    stream_push_handler,
    stream_complete_handler
)

from .privileges import (
    privileges_grantview_handler,
    privileges_grantappend_handler,
    privileges_granthost_handler,
    privileges_grantdiscover_handler,
    privileges_grantmodifyrights_handler,
    privileges_revokeview_handler,
    privileges_revokeappend_handler,
    privileges_revokehost_handler,
    privileges_revokediscover_handler,
    privileges_revokemodifyrights_handler,
)

from .zefhub import (
    zefhub_login_handler,
    zefhub_logout_handler,
)

from .tag import (
    graph_tag_handler,
    rae_tag_handler,
)

from .sync import (
    graph_sync_handler,
)

from .graph import (
    graph_take_transactor_role_handler,
    graph_release_transactor_role_handler,
    graph_transaction_handler,
    graph_load_handler,
)


from .clipboard import (
    clipboard_copy_to_handler,
    clipboard_copy_from_handler,
)

from .local_file import (
    read_localfile_handler,
    readbinary_localfile_handler,
    write_localfile_handler,
    save_localfile_handler,
    load_localfile_handler,
    system_open_with_handler,
    monitor_path_handler,
)

from .zef_studio import (
    studio_start_server_handler,
    studio_stop_server_handler,
)

# note the ".d" to access the tuple of Strings!
_effect_handlers = {
    FX.HTTP.StartServer.d: http_start_server_handler,
    FX.HTTP.StopServer.d: http_stop_server_handler,
    FX.HTTP.SendResponse.d: http_send_response_handler,
    FX.HTTP.Request.d: http_send_request_handler,
    
    FX.Websocket.ConnectToServer.d: websocket_connect_to_server_handler,
    FX.Websocket.StartServer.d: websocket_start_server_handler,
    FX.Websocket.StopServer.d: websocket_stop_server_handler,
    FX.Websocket.SendMessage.d: websocket_send_message_handler,
    FX.Websocket.CloseConnections.d: websocket_close_connections_handler,
    
    FX.GraphQL.StartServer.d: graphql_start_server_handler,
    FX.GraphQL.StopServer.d: graphql_stop_server_handler,
    FX.GraphQL.StartPlayground.d: graphql_start_playground_handler,
    FX.GraphQL.StopPlayground.d: graphql_stop_playground_handler,
    FX.GraphQL.GenerateSchemaString.d: graphql_generate_schema_string_handler,

    FX.Studio.StartServer.d: studio_start_server_handler,
    FX.Studio.StopServer.d: studio_stop_server_handler,
    
    FX.Stream.CreatePushableStream.d: stream_create_pushable_stream_handler,
    FX.Stream.Push.d: stream_push_handler,
    FX.Stream.Complete.d: stream_complete_handler,

    FX.Privileges.GrantView.d: privileges_grantview_handler,
    FX.Privileges.GrantAppend.d: privileges_grantappend_handler,
    FX.Privileges.GrantHost.d: privileges_granthost_handler,
    FX.Privileges.GrantDiscover.d: privileges_grantdiscover_handler,
    FX.Privileges.GrantModifyRights.d: privileges_grantmodifyrights_handler,
    FX.Privileges.RevokeView.d: privileges_revokeview_handler,
    FX.Privileges.RevokeAppend.d: privileges_revokeappend_handler,
    FX.Privileges.RevokeHost.d: privileges_revokehost_handler,
    FX.Privileges.RevokeDiscover.d: privileges_revokediscover_handler,
    FX.Privileges.RevokeModifyRights.d: privileges_revokemodifyrights_handler,

    FX.Graph.Tag.d: graph_tag_handler,
    FX.Graph.Sync.d: graph_sync_handler,
    FX.Graph.TakeTransactorRole.d: graph_take_transactor_role_handler,
    FX.Graph.ReleaseTransactorRole.d: graph_release_transactor_role_handler,
    FX.Graph.Transact.d: graph_transaction_handler,
    FX.Graph.Load.d: graph_load_handler,

    
    FX.Clipboard.CopyTo.d: clipboard_copy_to_handler,
    FX.Clipboard.CopyFrom.d: clipboard_copy_from_handler,

    FX.LocalFile.Read.d:  read_localfile_handler,
    FX.LocalFile.ReadBinary.d:  readbinary_localfile_handler,
    FX.LocalFile.Load.d:  load_localfile_handler,
    FX.LocalFile.Write.d: write_localfile_handler,
    FX.LocalFile.Save.d:  save_localfile_handler,
    FX.LocalFile.SystemOpenWith.d: system_open_with_handler,
    FX.LocalFile.MonitorPath.d: monitor_path_handler,

    FX.ZefHub.Login.d: zefhub_login_handler,
    FX.ZefHub.Logout.d: zefhub_logout_handler,

    FX.RAE.Tag.d: rae_tag_handler,
}
