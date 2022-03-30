#=============================================================================== WS handlers =========================================================================================================
from .fx_types import Effect

def websocket_start_server_handler(eff: Effect):
    print(f"websocket_start_handler called with eff={eff}")


def websocket_stop_server_handler(eff: Effect):
    print(f"websocket_stop_handler called")


def websocket_send_message_handler(eff: Effect):
    print(f"websocket_send_message_handler called")


def websocket_close_connections_handler():
    print(f"websocket_close_connections_handler called")


