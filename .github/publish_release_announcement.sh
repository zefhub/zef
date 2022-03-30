#!/bin/bash

if [[ -z "$GITHUB_REF" || -z "$S3_PATH" || -z "$MATTERMOST_URL" || -z "$CHANNEL" ]] ; then
    echo "Not all required env variables given"
    exit 1
fi

sed -e "s/_VERSION_/v${GITHUB_REF##*/v}/g" \
    -e 's!_DOWNLOAD_URL_!http://'${S3_PATH}'!g' \
    -e "s/_CHANGELOG_/$2/g" \
    -e "s/_CHANNEL_/${CHANNEL}/g" \
    release_template.json > release.json || exit 1
curl -i -X POST -H 'Content-Type: application/json' -d '@release.json' $MATTERMOST_URL
