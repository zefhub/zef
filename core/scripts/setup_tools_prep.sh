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


set -e

echo $(git rev-parse HEAD) > zef/packaged_git_commit

(cd cmake_build && make install)

# Need to make sure that the zeftypes are in the zefdb folder for fallbacks.
cp templates/zeftypes_bootstrap_??.json zef/
cp zeftypes_??.json zef/
cp cmake_install/lib/libzef* zef
cp cmake_install/lib/pyzef* zef
cp cmake_install/share/zefDB/auth.html zef