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

from ...pyzef.internals import login, login_manual, logout
from ..VT import Error
from .fx_types import Effect

def zefhub_login_handler(eff: Effect):
    try:
        jupyter_shell = False
        try:
            import IPython
            shell = IPython.get_ipython()
            if shell is not None and shell.__class__.__name__ != 'TerminalInteractiveShell':
                jupyter_shell = True
        except:
            pass
        comments = """=================================
    You are now logged in to ZefHub. You can synchronize graphs which will enable them to be stored on
    ZefHub. Any ETs, RT, ENs and KWs you create will also be synchronized with ZefHub.

    Note: your credentials have been stored in the zef config directory.
    By default these will be used to automatically connect to ZefHub on import of the zef module.
    If you would like to change this behavior, please see the `config` zefop for more information.
    ================================="""

        if 'auth_key' in eff:
            login_manual(eff['auth_key'])
            comments = """=================================
    You are logged in as a guest user, which allows you to view public graphs but
    does not allow for synchronising new graphs with ZefHub.

    Disclaimer: any ETs, RTs, ENs, and KWs that you query will be stored with ZefHub.
    ================================="""
        else:
            if jupyter_shell:
                print("=================================")
                print("This command starts a local libzef login web server. If you are running inside")
                print("of a jupyter notebook then this will start the auth prompt on the server where")
                print("jupyter is running.")
                print()
                print("If you are using jupyter through a Google Colab server or a different JupyterHub")
                print("server, then this will not be useful to you. In fact, we do not recommend you")
                print("login with your credentials on a remote server.")
                print()
                print("You can instead login as a guest user using `login_as_guest | run`")
                print()
                print("Advanced users: If you are sure you would like to login with your credentials,")
                print("then look for workarounds involving environment variables and manual login commands.")
                print("=================================")
            login()

        if jupyter_shell:
            print(comments)
            # TODO: Report user account here
            return {"success": True}
        else:
            # TODO: Report user account here
            return {"success": True,
                    "comments": comments}
    except Exception as e:
        return Error(f'executing FX.ZefHub.Login for effect {eff}:\n{repr(e)}')


def zefhub_logout_handler(eff: Effect):
    try:
        logout()
        print("Warning, after logging out all graphs will have been unloaded. Any accesses of a ZefRef")
        print("referencing these old graphs will cause a segmentation fault!")
        return {"success": True}
    except Exception as e:
        return Error(f'executing FX.ZefHub.Login for effect {eff}:\n{repr(e)}')
