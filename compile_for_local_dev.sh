#!/bin/bash

echo "pip-ing"
# Use tomlq to extract the build requirements
if ! which tomlq ; then
    python3 -m pip install yq || exit 1
fi

# Q and z are here to split using quotes and then remove the quotes in the resultant array
# packages=( ${(Q)${(z)$(tomlq -r '."build-system".requires | @sh' python/pyproject.toml)}} )
jqout=$(tomlq -r '."build-system".requires | @sh' python/pyproject.toml)
eval "packages=( $jqout )"
echo "$jqout" "${packages[@]}"
echo "${#jqout}" "${#packages[@]}"
echo "Packages to install: ${packages[@]}"
python3 -m pip install "${packages[@]}" || exit 1
# Install the runtime requirements
# python3 -m pip install -qr requirements.txt || exit 1



(
    cd python/pyzef || exit 1

    [ -d build ] || mkdir build || exit 1
    cd build
    cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=$(realpath ../../../core) || exit 1

    if which nproc ; then
        np="$(expr $(nproc))"
    elif which sysctl ; then
        np="$(expr $(sysctl -n hw.logicalcpu))"
    else
        np=""
    fi
    make -j $np || exit 1
)

ln -fs $(realpath python/pyzef/build/pyzef.* --relative-to=python/zef) python/zef/