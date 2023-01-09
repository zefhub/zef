#!/bin/bash
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

$ErrorActionPreference="Stop"

python -mpip install setuptools pybind11 cogapp pyfunctional
python -mpip -r python/requirements.txt

cd python\pyzef

mkdir -Force build
cd build

# We need to detect the version that is given with `python` so that cmake
# doesn't automatically go for a more recent version. This seems to be necessary
# when using the github CI action setup-python but should also help with user
# installs. There might be a more portable way to do this however...
cmake .. -DPython3_EXECUTABLE="$((Get-Command python).Path)" -DCMAKE_BUILD_TYPE=Release "-DZef_DIR=$((Resolve-Path ../../../core).ToString())" -DLIBZEF_PYZEF_BUNDLED=TRUE -DLIBZEF_STATIC=TRUE -DLIBZEF_FORCE_ASSERTS=TRUE


cmake --build . --config Release -j

copy Release\pyzef.* ..\..\zef\
copy ..\..\..\core\auth.html ..\..\zef\