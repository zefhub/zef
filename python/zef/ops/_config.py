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


__all__ = [
    "config"
]

import os
from pathlib import Path
import yaml
from ..core.op_implementations.dispatch_dictionary import _op_to_functions
from ..core import *
from ..core._ops import *

from dataclasses import dataclass
import typing
@dataclass
class ConfigItem:
    path: str
    # These two types will have to be handled later on
    typ: typing.Any
    default: typing.Any
    env_var: str
    doc: str
    

config_spec = [
    # Options are "false", "auto", "always"
    ConfigItem("login.autoConnect", None, "auto", "ZEFDB_LOGIN_AUTOCONNECT", "Whether zefdb should automatically connect to ZefHub on start of the butler."),
    # In the future, this should be true not "true"
    ConfigItem("butler.autoStart", bool, "true", "ZEFDB_BUTLER_AUTOSTART", "Whether the butler will automatically be started on import of the zefdb module."),
]
config_spec = {x.path: x for x in config_spec}
# Anything that has been manually set in this session will override any environment variable etc...
session_overrides = {}


config_file_path = Path.home() / ".zef" / "config.yaml"


def config_file_exists():
    return os.path.exists(config_file_path)

def ensure_config_file():
    if os.path.exists(config_file_path):
        return
    config_file_path.parent.mkdir(parents=True, exist_ok=True)
    # if not os.access(str(config_file_path), os.W_OK):
    #     raise Exception(f"Don't have permissions to write to config file path: {config_file_path}") 
    with open(config_file_path, 'w'):
        pass

def nested_set(dic, keys, value):
    for key in keys[:-1]:
        dic = dic.setdefault(key, {})
    dic[keys[-1]] = value


def get_config(key=""):
    # TODO: Danny did this quickly, but it would only work for specific keys.
    # Need to handle wildcards/group selections.
    if key in session_overrides:
        return session_overrides[key]
    if key in config_spec:
        spec = config_spec[key]
        import os
        if spec.env_var in os.environ:
            # TODO: Handle conversion from string here
            return os.environ[spec.env_var]

    # Check if config file exists
    if config_file_exists():
        with open(config_file_path, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
                if config is None:
                    # TODO: Handle warnings about improper config here
                    return None

                # Check if key is empty or wildcard
                if key == "" or key == "*":
                    return config

                # Check if key is in config and return value
                for k in key.split("."):
                    if k not in config:
                        config = None
                        break
                    config = config[k]
                if config is not None:
                    return config
            except yaml.YAMLError as exc:
                print(exc)

    # This is the default option - we also assume that by getting here we must
    # have a valid config key, so error if nothing matches.
    if key not in config_spec:
        raise Exception(f"Unknown config option {key}")

    return config_spec[key].default
        
    


def set_config(key, val):
    ensure_config_file()
    try:
        with open(config_file_path, 'r') as stream:
            config = yaml.safe_load(stream)
            if key == "":
                return config
            if config is None:
                config = {}

            nested_set(config, key.split("."), val)
            yamlConfig = yaml.safe_dump(config, default_flow_style=False)
        with open(config_file_path, 'w') as filehandle:
            filehandle.write(yamlConfig)
    except yaml.YAMLError as exc:
        print(exc)

    if key in config_spec:
        session_overrides[key] = val


def list_config():
    if config_file_exists():
        with open(config_file_path, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
                # TODO: Print ascii table?
            except yaml.YAMLError as exc:
                print(exc)


def config_implementation(payload, action):
    if action == KW.set:
        assert isinstance(payload, tuple)
        assert len(payload) == 2
        assert isinstance(payload[0], str)
        set_config(payload[0], payload[1])
    elif action == KW.get:
        assert isinstance(payload, str)
        return get_config(payload)
    else:
        raise NotImplementedError("Action {} not implemented".format(action))


def config_typeinfo(v_tp):
    return VT.Any


_op_to_functions[RT.Config] = (config_implementation, config_typeinfo)

config = ZefOp(((RT.Config, ()), ))
