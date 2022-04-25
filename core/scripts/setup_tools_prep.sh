#!/bin/bash

set -e

echo $(git rev-parse HEAD) > zef/packaged_git_commit

(cd cmake_build && make install)

# Need to make sure that the zeftypes are in the zefdb folder for fallbacks.
cp templates/zeftypes_bootstrap_??.json zef/
cp zeftypes_??.json zef/
cp cmake_install/lib/libzef* zef
cp cmake_install/lib/pyzef* zef
cp cmake_install/share/zefDB/auth.html zef