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


echo "pip-ing"
if ! which jq > /dev/null ; then
    echo "jq needs to be installed to use this script. If you are on macos, please run `brew install jq`. If you are on linux use your package manager to install jq (e.g. `sudo apt-get install jq` or `sudo pacman -S jq`)"
    exit 1
fi
if ! which realpath > /dev/null ; then
    echo "realpath needs to be installed to use this script. If you are on macos, please run `brew install coreutils`. If you are on linux this is weird, realpath should already be available!"
    exit 1
fi

# Use tomlq to extract the build requirements
if ! which tomlq > /dev/null ; then
    python3 -m pip install yq || exit 1
fi

# Q and z are here to split using quotes and then remove the quotes in the resultant array
# packages=( ${(Q)${(z)$(tomlq -r '."build-system".requires | @sh' python/pyproject.toml)}} )
jqout=$(tomlq -r '."build-system".requires | @sh' python/pyproject.toml)
eval "packages=( $jqout )"
python3 -m pip install "${packages[@]}" || exit 1
# Install the runtime requirements
# python3 -m pip install -qr requirements.txt || exit 1



(
    cd python/pyzef || exit 1

    [ -d build ] || mkdir build || exit 1
    cd build

    if which ninja > /dev/null ; then
        export CMAKE_GENERATOR=Ninja
        CMAKE_ARGS=""
    else
        if which nproc ; then
            np="$(expr $(nproc))"
        elif which sysctl ; then
            np="$(expr $(sysctl -n hw.logicalcpu))"
        else
            np=""
        fi
        CMAKE_ARGS="-j $np"
    fi
    cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=$(realpath ../../../core) || exit 1

    cmake --build . $CMAKE_ARGS || exit 1
) || exit 1

ln -fs $(realpath python/pyzef/build/pyzef.* --relative-to=python/zef) python/zef/
