#!/bin/bash

if [ -z "$GIT_CREDENTIALS" -a -f ~/.git-credentials ] ; then
    echo "Grabbing git credentials from ~/.git-credentials"
    export GIT_CREDENTIALS=$(cat ~/.git-credentials)
fi
export CODEARTIFACT_TOKEN=$(aws codeartifact get-authorization-token --domain synchronous --query authorizationToken --output text)

sed -e 's/julia-python-base:jl1.5-py3.8/julia-python-base:jl1.5-py3.9/' Dockerfile.build_wheel > Dockerfile.build_wheel.py39 || exit 1

docker build -f Dockerfile.build_wheel.py39 \
       --build-arg CODEARTIFACT_TOKEN \
       --build-arg GIT_CREDENTIALS \
       --build-arg VERSION_STRING \
       --build-arg ZEFHUB_AUTH_KEY \
       --build-arg WITH_JULIA \
       . "$@"
