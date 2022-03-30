#!/bin/bash

python3 create_tokens_files.py || exit 1

echo "Output for debugging"
wc -l early.tokens

nlines=$(wc -l all_created.tokens | awk '{print $1}')

if [[ $nlines != "0" ]] ; then
    echo "At least $nlines tokens!"
    exit 2
fi