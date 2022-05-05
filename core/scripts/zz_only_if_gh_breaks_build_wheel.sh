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


if [ -z "$VERSION_STRING" ] ; then
    export VERSION_STRING=$(git tag --points-at HEAD)
    if [[ -z "$VERSION_STRING" ]] ; then
        echo "Git HEAD doesn't have a tag - either set one or specify VERSION_STRING manually"
        exit 1
    fi
    echo "Automatic version $VERSION_STRING found"
fi

if [ -z "$GIT_CREDENTIALS" -a -f ~/.git-credentials ] ; then
    echo "Grabbing git credentials from ~/.git-credentials"
    export GIT_CREDENTIALS=$(cat ~/.git-credentials)
fi
export CODEARTIFACT_TOKEN=$(aws codeartifact get-authorization-token --domain synchronous --query authorizationToken --output text)

docker build -f Dockerfile.build_wheel \
       --build-arg CODEARTIFACT_TOKEN \
       --build-arg GIT_CREDENTIALS \
       --build-arg ZEFHUB_AUTH_KEY \
       --build-arg VERSION_STRING \
       --build-arg WITH_JULIA \
       . "$@"
