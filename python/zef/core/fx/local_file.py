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
from ..VT import Error, Image
import yaml 
import json 
import toml
import pandas as pd
import io

#TODO Docstring!

def read_localfile_handler(eff: Effect):
    """
    Reads the file as a string. Returns a str object.
    To read as a binary file, use FX.LocalFile.ReadBinary.
    >>> FX.LocalFile.Read(filename='my_file.txt')

    Response example:
    {
        'content': some_dict,
        'filename': 'my_file.txt',
    }
    """
    try:
        filename  = eff["filename"]
        with open(filename, "r") as f:
            f = f.read()
        return {"content": f, "filename": filename}
    except Exception as e:
        raise RuntimeError(f"Error reading file in FX.LocalFile.Read for effect={eff}:\n {repr(e)}")

def readbinary_localfile_handler(eff: Effect):
    """
    Reads a localfile in binary mode and returns the content as bytes.
    >>> FX.LocalFile.ReadBinary(filename='my_file.txt')

    Response example:
    {
        'content': some_dict,
        'filename': 'my_file.txt',
    }
    """
    try:
        filename  = eff["filename"]
        with open(filename, "rb") as f:
            f = f.read()
        return {"content": f, "filename": filename}
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
    - gif

    Response example:
    {
        'content': some_dict,
        'format': 'json',
        'filename': 'my_file.json',
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


        if format in {"svg", "png", "jpg", "jpeg", "gif"}:
            with open(filename, "rb") as f:
                content = f.read()
            content = Image(content, format)
        else:
            with open(filename, "rb") as f:
                content = f.read()  
            if format in {"yaml", "yml"}:
                content = yaml.safe_load(content)
            elif format == "toml":
                content = toml.loads(content)
            elif format == "csv":
                content = pd.read_csv(io.StringIO(content), **settings)
            elif format == "json":
                content = json.loads(content)

        return {"content": content, "format": format, "filename": filename}
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

    mode = "wb"  if isinstance(content, bytes) else "w"
    with open(filename, mode) as f: f.write(content)
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



from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
class ZefEventHandler(FileSystemEventHandler):
    def __init__(self, created = None, modified = None, moved = None, deleted = None):
        super().__init__()
        self.created = created
        self.modified = modified
        self.moved = moved
        self.deleted = deleted
    def on_created(self, event):
        super().on_created(event)
        if self.created: self.created(event)
        
    def on_modified(self, event):
        super().on_modified(event)
        if self.modified: self.modified(event)
    def on_moved(self, event):
        super().on_moved(event)
        if self.moved: self.moved(event)
    
    def on_deleted(self, event):
        super().on_deleted(event)
        if self.deleted: self.deleted(event)

def monitor_path_handler(eff: Effect):
    """
    Watches for changes in files and nested directories at given path for changes.
    These changes include, file creation, modification, deletion and moving.

    If a change handler is set, once an event of the type is triggered the handler is called.

    Example
    =======

    {
            'type': FX.LocalFile.MonitorPath,
            'path': fpath,
            'recursive': True,           # default is False
            'created_handler': f1,       # default None
            'modified_handler': f2,      # default None
            'moved_handler': f3,         # default None
            'deleted_handler': f4,       # default None
    }
    """
    assert 'path' in eff, "path is required for FX.LocalFile.MonitorPath"
    path = eff['path']
    recursive = eff.get('recursive', False)

    created_handler = eff.get("created_handler", None)
    modified_handler = eff.get("modified_handler", None)
    moved_handler = eff.get("moved_handler", None)
    deleted_handler = eff.get("deleted_handler", None)

    event_handler = ZefEventHandler(created = created_handler, modified = modified_handler, moved = moved_handler, deleted = deleted_handler)

    observer = Observer()
    observer.schedule(event_handler, path, recursive=recursive)
    observer.start()

    return {"observer_object": observer}