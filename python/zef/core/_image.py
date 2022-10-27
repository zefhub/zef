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

from .. import report_import
report_import("zef.core.image")

import zstd

class Image_:
    def __init__(self, data, format='svg'):
        self.format = format
        self.compression = 'zstd'
        self.buffer = zstd.compress(data)
    def _repr_svg_(self):
        # this function must return a str
        return zstd.decompress(self.buffer).decode("utf-8") if self.format=='svg' else None
    def _repr_png_(self):
        # this function must return bytes
        return zstd.decompress(self.buffer) if self.format in {'gif','png'} else None
    def _repr_jpeg_(self):
        # this function must return bytes
        return zstd.decompress(self.buffer) if self.format in {'jpeg','jpg'} else None

    def _view(self, format=None):
        if format is None:
            format = self.format

        import tempfile, os
        fd,filename = tempfile.mkstemp(f".{format}")
        file = os.fdopen(fd, 'wb')
        try:
            # TODO: In the future, do a conversion of file format
            if format == "svg":
                data = self._repr_svg_()
            elif format == {"png", 'gif'}:
                data = self._repr_png_()
            elif format in {'jpeg','jpg'}:
                data = self._repr_jpeg_()

            if data is None:
                raise Exception(f"Can't convert '{self.format}' to '{format}'")
                
            if isinstance(data, str):
                data = data.encode("utf-8")
            file.write(data)
        finally:
            file.close()

        from . import FX, Effect
        from ._ops import run
        {
            "type": FX.LocalFile.SystemOpenWith,
            "filepath": filename
        } | run

from .VT import make_VT
make_VT("Image", pytype=Image_)