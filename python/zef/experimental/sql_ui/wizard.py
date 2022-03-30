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

import os
backend = os.environ.get("SQL_UI_BACKEND", None)
if backend is None:
    try:
        import sdl2
        backend = "sdl2"
    except ImportError:
        pass
# The pygame backend seems broken
# if backend is None:
#     try:
#         import pygame
#         backend = "pygame"
#     except ImportError:
#         pass
if backend is None:
    try:
        import glfw
        backend = "glfw"
    except ImportError:
        pass

if backend is None:
    raise ImportError("""Can't import either sdl2 or glfw for imgui backend support in sql_ui wizard.

Please install either of these backends using
`pip install 'imgui[sdl2]'`
or
`pip install 'imgui[glfw]'`

You may have to install the corresponding libraries on your system as well, e.g.
`brew install SDL2`.
""")

import imgui
from . import ui 

if __name__ == "__main__":
    import sys
    assert len(sys.argv) == 2, "Need one argument of the yaml file to edit."
    filepath = sys.argv[1]


    if backend == "sdl2":
        from .wizard_sdl2 import main
    elif backend == "pygame":
        from .wizard_pygame import main
    elif backend == "glfw":
        from .wizard_glfw import main
    main("SQL Import Wizard", ui.init_state, ui.render, filepath)