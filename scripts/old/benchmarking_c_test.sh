#!/bin/zsh
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


export ZEFDB_LOGIN_AUTOCONNECT=FALSE

tag=${1:-UNTAGGED}

echo "# num_entities num_batches time_in_ms" >> bmarks.dat
for iter in $(seq 10) ; do
    for n in 10 100 1000 ; do
        for m in 1 10 100 ; do
            time=$(src_cpp/c_tests/c_test $n $m 2>&1 | grep "Time was" | awk '{print $3}' && exit ${pipestatus[1]})
            if [[ $? != 0 ]] ; then
               echo "The c_test failed!"
               exit 1
            fi

            echo $n $m $tag $time >> bmarks.dat
            echo $n $m $tag $time
        done
    done
done