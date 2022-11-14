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


THISDIR=$(dirname $0)

TARGET_FILE=$(mktemp --tmpdir pyicheck.XXX.pyi)

python3 $THISDIR/../python/zef/core/VT/generate_pyi.py $TARGET_FILE || exit 1

if ! diff --ignore-all-space ${TARGET_FILE} $THISDIR/../python/zef/core/VT/__init__.pyi ; then
    echo "__init__.pyi file needs to be regenerated inside of the VT directory"
    exit 1
fi

exit 0