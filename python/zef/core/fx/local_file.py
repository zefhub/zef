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
import yaml 
import json 
import toml
import  pandas as pd
import io
from ..image import Image

#TODO Docstring!

def read_localfile_handler(eff: Effect):
    """
    Unopinionated reading of file. Returns a Bytes object.
    >>> FX.LocalFile.Read(filename='my_file.txt')

    Response example:
    {
        'content': some_dict,
    }
    """
    try:
        filename  = eff["filename"]
        f = open(filename, "r")
        f = f.read()
        return {"content": f}
    except Exception as e:
        raise RuntimeError(f"Error reading file in FX.LocalFile.Read for effect={eff}:\n {repr(e)}")


def load_localfile_handler(eff: Effect):
    """
    Opinionated loading of file: it parses based on the file extension.
    If you want plain reading of a file, use 
    >>> FX.LocalFile.Load(filename='my_file.json')

    Supported formats:
    - json
    - csv
    - toml
    - yaml / yml
    - svg
    - png
    - jpg
    - jpeg

    Response example:
    {
        'content': some_dict,
        'format': 'json',
    }
    """
    try:
        filename  = eff["filename"]
        settings  = eff.get("settings", {})
        format    = eff.get("format", None)

        if not format:
            if "." not in filename: return Error.ValueError("filename is missing an extension and a format wasn't provided!", filename)
            else: format = filename[filename.rindex(".") + 1:]
        elif "." not in filename: filename = filename + f".{format}"

        f = open(filename, "r")
        content = f.read()

        if format in {"svg", "png", "jpg", "jpeg"}:
            content = bytes(content, "UTF-8")
            content = Image(content, format)
        elif format in {"yaml", "yml"}:
            content = yaml.safe_load(content)
        elif format == "toml":
            content = toml.loads(content)
        elif format == "csv":
            content = pd.read_csv(io.StringIO(content), **settings)
        elif format == "json":
            content = json.loads(content)

        return {"content": content, "format": format}
    except Exception as e:
        return Error(f'executing FX.LocalFile.Load for effect {eff}:\n{repr(e)}')



def save_localfile_handler(eff: Effect):
    """
    Example Effect

    {
        'type':     FX.LocalFile.Save,
        'filename': fname,
        'content':  content,
        'settings': settings,
    }
    """
    try:
        content   = eff["content"]
        filename  = eff["filename"]
        settings  = eff.get("settings", {})

        if isinstance(content, Image):
            format = f'.{content.format}'
            if format not in {".svg", ".png", ".jpg", ".jpeg"}: return Error.ValueError(f'Image format needs to be one of these types {{".svg", ".png", ".jpg", ".jpeg"}} got {format} instead.')        
            filename = filename if format == (filename[-4:] or filename[-5:])  else filename + format
            import zstd
            with open(filename, 'wb') as f: f.write(zstd.decompress(content.buffer))
            
        elif "." in filename:
            format = filename[filename.rindex(".") + 1:]
            if format in {"yaml", "yml"}:
                with open(filename, 'w') as f: f.write(yaml.safe_dump(content))
            elif format == "toml":
                with open(filename, 'w') as f: f.write(toml.dumps(content))
            elif format == "csv":
                with open(filename, 'w') as f: f.write(content.to_csv(**settings))
            elif format == "json":
                with open(filename, 'w') as f: f.write(json.dumps(content))
            else:
                return Error.NotImplementedError(f'Unsupported writing to file type {format}!')
        else:
            filename += ".txt"
            with open(filename, 'w') as f: f.write(content)

        return {"filename": filename}
    except Exception as e:
        return Error(f'executing FX.LocalFile.Save for effect {eff}: {repr(e)}')


def write_localfile_handler(eff: Effect):
    """
    Example Effect

    {
        'type':     FX.LocalFile.Write,
        'filename': fname,
        'content':  content,
    }
    """
    content   = eff["content"]
    filename  = eff["filename"]

    with open(filename, "w") as file: file.write(content)
    return {"filename": filename}



def system_open_with_handler(eff: Effect):
    """
    This will open the file using the system's default handler application.

    Example
    =======

    {
            'type': FX.LocalFile.SystemOpenWith,
            'filepath': fname,
    }
    """
    try:
        import subprocess, os, platform
        if platform.system() == 'Darwin':       # macOS
            subprocess.Popen(('open', eff['filepath']))
        elif platform.system() == 'Windows':    # Windows
            os.startfile(eff['filepath'])
        else:                                   # linux variants
            subprocess.Popen(('xdg-open', eff['filepath']))

    except Exception as e:
        return Error(f'executing FX.LocalFile.SystemOpenWith for effect {eff}:\n{repr(e)}')

