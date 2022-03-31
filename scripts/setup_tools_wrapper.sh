#!/bin/bash

set -e


if [[ -z "$VERSION_STRING" ]] ; then
    VERSION_STRING=$(git tag --points-at HEAD)
    if [[ -z "$VERSION_STRING" ]] ; then
        echo "Git HEAD doesn't have a tag - either set one or specify VERSION_STRING manually"
        exit 1
    fi
    echo "Using VERSION_STRING=$VERSION_STRING"
    export VERSION_STRING
fi

echo $(git rev-parse HEAD) > zef/packaged_git_commit

# bash make_cleanslate.sh || true

# pip3 install -r requirements.txt
# cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$(pwd)/cmake_install -B cmake_build
# cmake . -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$(pwd)/cmake_install -B cmake_build

bash make_everything.sh

# if which nproc ; then
#     np="-j $(nproc)"
# else
#     np=""
# fi
# cmake --build cmake_build $np
# cmake --install cmake_build
(cd cmake_build && make install)

# Need to make sure that the zeftypes are in the zefdb folder for fallbacks.
cp templates/zeftypes_bootstrap_??.json zef/
cp zeftypes_??.json zef/
cp cmake_install/lib/libzef* zef
cp cmake_install/lib/pyzef* zef
cp cmake_install/share/zefDB/auth.html zef
    
python3 setup.py bdist_wheel
