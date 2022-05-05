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

python3 $THISDIR/create_tokens_files.py || exit 1

echo "Output for debugging"
wc -l early.tokens

nlines=$(wc -l early.tokens | awk '{print $1}')

if [[ $nlines != "0" ]] ; then
    echo "At least $nlines tokens!"
    exit 2
fi
