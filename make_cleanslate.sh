#!/bin/bash

rm -fr build
rm -fr cmake_build
rm -rf _cmake_deps
rm -fr cmake_install
rm -fr dist
rm -f zeftypes_??.json
rm -f pip_output.log
rm -f dummy_cog.tracker
find src_cpp -iname 'Manifest.toml' -delete
find src_cpp -iname '*.gen' -delete
# Annoyingly, some libs can pollute the image from temporary build things outside
# find zefdb -iname 'pyzef.*.so' -delete
rm -f zefdb/pyzef*
