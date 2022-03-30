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
from cogapp import Cog

import sys
search_path = sys.argv[1]
output_path = sys.argv[2]
tokens_path = sys.argv[3]

def find_files_of_type(path, filename_endings, directories_to_exclude={}):
    # Returns paths relative to the input path
    for dirpath,dirnames,filenames in os.walk(path):
        for filename in filenames:
            thispath = os.path.join(dirpath, filename)
            if os.path.splitext(thispath)[1] in filename_endings:
                yield os.path.relpath(thispath, path)

        for dirname in directories_to_exclude:
            while dirname in dirnames:
                dirnames.remove(dirname)

cog = Cog()
cog.options.bReplace = False
cog.options.bDeleteCode = True
cog.options.sPrologue = f"""
import cog
import json
import os

et_filename = os.path.join("{tokens_path}", "zeftypes_ET.json")
with open(et_filename) as F:
    et = json.loads(F.read())

rt_filename = os.path.join("{tokens_path}", "zeftypes_RT.json")
with open(rt_filename) as F:
    rt = json.loads(F.read())

kw_filename = os.path.join("{tokens_path}", "zeftypes_KW.json")
with open(kw_filename) as F:
    kw = json.loads(F.read())

en_filename = os.path.join("{tokens_path}", "zeftypes_EN.json")
with open(en_filename) as F:
    en = json.loads(F.read())

def enum_type(x):
    return x.split('.')[0]
def enum_val(x):
    return x.split('.')[1]
"""


#cog.options.verbosity = 0
for filename in find_files_of_type(path=search_path, filename_endings={'.cog'}):
    try:
        true_output = os.path.join(output_path, filename[:-len(".cog")] + ".gen")
        os.makedirs(os.path.dirname(true_output), exist_ok=True)
        cog.options.sOutputName = true_output + ".tmp"
        cog.processOneFile(os.path.join(search_path, filename))
        if not os.path.exists(true_output) or open(true_output + ".tmp").read() != open(true_output).read():
            print(filename, " changed")
            os.rename(true_output + ".tmp", true_output)
        else:
            os.unlink(true_output + ".tmp")
    except Exception as exc:
        print(f'An exception was raised when processing file "{filename}": {exc}')
        # Need this to fail for cmake to not continue on without a care.
        raise
