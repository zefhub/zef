#!/bin/bash


function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w /io/wheelhouse/
    fi
}

yum install -y openssl-devel libzstd-devel libcurl-devel

# apt-get update
# apt-get install -y libzstd-dev libssl-dev libcurl4-openssl-dev

# Compile wheels
# for PYBIN in /opt/python/*/bin; do
for Python_ROOT_DIR in /opt/python/* ; do
    PYBIN=${Python_ROOT_DIR}/bin
    echo "ROOT DIR IS: " $Python_ROOT_DIR
    PATH=$PYBIN:$PATH pip install -r /io/python/requirements_build.txt
    # PATH=$PYBIN:$PATH pip install cmake
    PATH=$PYBIN:$PATH pip wheel /io/python --no-deps -w wheelhouse/
    exit 1
done

# Bundle external shared libraries into the wheels
for whl in wheelhouse/*.whl; do
    repair_wheel "$whl"
done

# # Install packages and test
# for PYBIN in /opt/python/*/bin/; do
#     "${PYBIN}/pip" install python-manylinux-demo --no-index -f /io/wheelhouse
#     (cd "$HOME"; "${PYBIN}/nosetests" pymanylinuxdemo)
# done