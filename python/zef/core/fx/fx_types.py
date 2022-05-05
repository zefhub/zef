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


#---------------------------------------------------------------------------------------

# allow value based comparisons
class FXElement():
    def __init__(self, args):
        """the data is stored as an array, typically of length 2"""
        self.d = args
        
    def __repr__(self):
        return "FX." + '.'.join(self.d)

    # This is a check to make sure we align the strings with the variable names.
    # We could also use this to assign the strings in the first place
    def __set_name__(self, owner, name):
        assert self.d[0] == owner._name
        assert self.d[1] == name


    def __call__(self, **kwargs):
        """
        Allow using this type as a constructor function,
        which automatically injects its own type. Other
        key-value pairs are passed in as keyword arguments.
        e.g.
        >>> FX.HTTP.Request(url='zefhub.io')     # returns an Effect

        TODO: if the effect module provides validation functions that
        can be run, execute this before returning. If it fails, return an
        error.
        """
        return Effect({
            **kwargs,
            'type': self,
        })



class _TX_Class():
    _name = "TX"
    Transact = FXElement(('TX', 'Transact'))


class _HTTP_Class():
    _name = "HTTP"
    StartServer = FXElement(('HTTP', 'StartServer'))
    StopServer = FXElement(('HTTP', 'StopServer'))
    SendResponse = FXElement(('HTTP', 'SendResponse'))
    
    Request = FXElement(('HTTP', 'Request'))


class _Websocket_Class():
    _name = "Websocket"
    StartServer = FXElement(('Websocket', 'StartServer'))
    StopServer = FXElement(('Websocket', 'StopServer'))
    SendMessage = FXElement(('Websocket', 'SendMessage'))
    CloseConnections = FXElement(('Websocket', 'CloseConnections'))

class _Subprocess_Class():
    _name = "Subprocess"
    Start = FXElement(('Subprocess', 'Start'))
    Stop = FXElement(('Subprocess', 'Stop'))

class _ZefUI_Class():
    _name = "ZefUI"
    StartApp = FXElement(('ZefUI', 'StartApp'))
    StartServer = FXElement(('ZefUI', 'StartServer'))
    StopServer = FXElement(('ZefUI', 'StopServer'))


class _GraphQL_Class():
    _name = "GraphQL"
    StartServer = FXElement(('GraphQL', 'StartServer'))
    StopServer = FXElement(('GraphQL', 'StopServer'))
    StartPlayground = FXElement(('GraphQL', 'StartPlayground'))
    StopPlayground = FXElement(('GraphQL', 'StopPlayground'))


class _LocalFile_Class():
    _name = "LocalFile"
    Read = FXElement(('LocalFile', 'Read'))
    Load = FXElement(('LocalFile', 'Load'))
    Write = FXElement(('LocalFile', 'Write'))
    Save = FXElement(('LocalFile', 'Save'))
    SystemOpenWith = FXElement(('LocalFile', 'SystemOpenWith'))
    

class _S3_Class():
    _name = "S3"
    Read = FXElement(('S3', 'Read'))
    Write = FXElement(('S3', 'Write'))
    
    
class _FX_Class():
    _name = "FX"
    AddEffectHandler = FXElement(('FX', 'AddEffectHandler'))

class _Graph_Class():
    _name = "Graph"
    Tag = FXElement(('Graph', 'Tag'))
    Sync = FXElement(('Graph', 'Sync'))
    TakeTransactorRole = FXElement(('Graph', 'TakeTransactorRole'))
    ReleaseTransactorRole = FXElement(('Graph', 'ReleaseTransactorRole'))
    # Or does it make more sense to write FX.Tag.Graph? Tagging of RAEs definitely belongs into a GraphDelta / graph tx though

class _Stream_Class():
    _name = "Stream"
    CreatePushableStream = FXElement(('Stream', 'CreatePushableStream'))
    Push = FXElement(('Stream', 'Push'))
    Complete = FXElement(('Stream', 'Complete'))
    

class _Privileges_Class():
    _name = "Privileges"
    GrantView = FXElement(('Privileges', 'GrantView'))
    GrantAppend = FXElement(('Privileges', 'GrantAppend'))
    GrantHost = FXElement(('Privileges', 'GrantHost'))
    GrantDiscover = FXElement(('Privileges', 'GrantDiscover'))
    GrantModifyRights = FXElement(('Privileges', 'GrantModifyRights'))
    RevokeView = FXElement(('Privileges', 'RevokeView'))
    RevokeAppend = FXElement(('Privileges', 'RevokeAppend'))
    RevokeHost = FXElement(('Privileges', 'RevokeHost'))
    RevokeDiscover = FXElement(('Privileges', 'RevokeDiscover'))
    RevokeModifyRights = FXElement(('Privileges', 'RevokeModifyRights'))

class _ZefHub_Class():
    _name = "ZefHub"
    Login = FXElement(('ZefHub', 'Login'))
    Logout = FXElement(('ZefHub', 'Logout'))


# using the following, some IDEs don't pick up the class members
# class _Placeholder_Class():
#     def __init__(self, group_name:str, v):
#         self.A = FXElement(('Placeholder', 'A'))
#         setattr(self, 'B', FXElement(('Placeholder', 'B')))


class _Clipboard_Class():
    _name = "Clipboard"
    CopyTo = FXElement(('Clipboard', 'CopyTo'))
    CopyFrom = FXElement(('Clipboard', 'CopyFrom'))     # prefer this over "paste", which is imperative to 'paste' it somewhere, i.e. plop. We can also just get a value out, i.e. this is a data source in the imperative shell

class _RAE_Class():
    _name = "RAE"
    Tag = FXElement(('RAE', 'Tag'))



class _FX_Class:
    TX = _TX_Class()
    HTTP = _HTTP_Class()
    Websocket = _Websocket_Class()
    Subprocess = _Subprocess_Class()
    GraphQL = _GraphQL_Class()
    ZefUI = _ZefUI_Class()
    LocalFile = _LocalFile_Class()
    S3 = _S3_Class()
    Graph = _Graph_Class()
    FX = _FX_Class()
    Stream = _Stream_Class()
    Privileges = _Privileges_Class()
    Clipboard = _Clipboard_Class()
    ZefHub = _ZefHub_Class()
    RAE = _RAE_Class()


FX = _FX_Class()

_group_types = [ _Clipboard_Class,_FX_Class,_GraphQL_Class,_Graph_Class,_HTTP_Class,_LocalFile_Class,_Privileges_Class,_S3_Class,_Stream_Class,_TX_Class,_Websocket_Class,_ZefHub_Class,_ZefUI_Class]
#---------------------------------------------------------------------------------------

_warn_did_not_run = True

class _Effect_Class:
    def __init__(self, d: dict):
        # TODO: Add checks that this is a valid effect
        self.d = d
        self._been_run = False
        
    def __repr__(self):
        return f"Effect({self.d})"

    def __del__(self):
        if not _warn_did_not_run:
            return
        if not self._been_run:
            import logging
            # import traceback
            # traceback.print_stack()
            logging.warning(f"Effect of type {self.d.get('type')} was garbage collected without being run. Set {__name__}._warn_did_not_run to False to disable this warning.")

    def __call__(self):
        raise TypeError("Effects are not callable. Please use `eff | run` instead.")

    def __iter__(self):
        """ 
        Allow casting to dictionaries: just return the internal
        dictionary. An effect is a thin wrapper around a dictionary
        with the constructor allowing initial checking of requirements.
        
        Equivalent to "peel"
        """
        return iter(self.d.items())

    

# just a function, since Effect will just become a value.
# same syntax as a regular constructor and equivalent to to_effect.
# The latter is the more usual option to use in the context of piping
def Effect(*args, **kwargs):
    """
    Effects can be constructed via keyword arguments
    or by passing in a dictionary.
    
    ---- Examples ----
    Effect(type=FX.Subprocess.Start)
    Effect({'type': FX.Subprocess.Start})
    """
    if args!=():
        assert len(kwargs) == 0
        assert len(args) == 1
        assert isinstance(args[0], dict)
        return _Effect_Class(args[0])

    else:
        return _Effect_Class(kwargs)

