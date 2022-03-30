#!/bin/bash

# cmake . -DCMAKE_BUILD_TYPE=Release -B cmake_build || exit 1
if [ ! -d cmake_build ] || [ "$#" -ne 0 ] ; then
    if [ -z "$@" ] ; then
        cmake_extras=( "-DCMAKE_BUILD_TYPE=Release" "-DCMAKE_INSTALL_PREFIX=$(pwd)/cmake_install" )
    else
        cmake_extras=( "$@" )
    fi

    echo "pip-ing"
    python3 -m pip install -qr requirements.txt || exit 1

    mkdir cmake_build
    (
        cd cmake_build
        cmake .. "${cmake_extras[@]}"
    ) || exit 1
fi

if which nproc ; then
    # np="$(expr $(nproc) / 2)"
    np="$(expr $(nproc))"
elif which sysctl ; then
    # np="$(expr $(sysctl -n hw.logicalcpu) / 2)"
    np="$(expr $(sysctl -n hw.logicalcpu))"
else
    np=""
fi
# cmake --build cmake_build $np || exit 1
(cd cmake_build ; make -j $np) || exit 1
