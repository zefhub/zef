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
report_import("zef.core.colors")
from .VT import *


def valid_hex_color_code(color):
    if len(color) not in {6,7}:
        return False
    try:
        if len(color) == 7: color = color[1:]
        int(color, 16)
    except ValueError:
        return False
    return True
    
ValidHEXColorCode = Is[valid_hex_color_code]
Color = UserValueType("Color", String, ValidHEXColorCode, forced_uid="10252a53a03086b7")


colors = {
    "red": Color("#E45D51"),
    "pink": Color("#FF647C"),
    "magenta": Color("#E13383"),
    "purple": Color("#AE2564"),
    "violet": Color("#BE52F3"),
    "lavender": Color("#DAA5F5"),
    "blue": Color("#A6AFFC"),
    "turquoise": Color("#4AA9C5"),
    "cyan": Color("#449ED4"),
    "indigo": Color("#3E66FB"),
    "teal": Color("#3C8A8A"),
    "green": Color("#01C48C"),
    "mint": Color("#7DE0C4"),
    "lime": Color("#3CC13B"),
    "yellow": Color("#FECF5B"),
    "gold": Color("#E3B94A"),
    "peach": Color("#FFE29E"),
    "orange": Color("#FFA26C"),
    "coral": Color("#FFC7A6"),
    "salmon": Color("#FDAFBC"),
}

