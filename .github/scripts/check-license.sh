#!/bin/bash

addlicense -v -c "Synchronous Technologies Pte Ltd" -f ../assets/apache-license.md -ignore "core/build/**" -ignore "python/pyzef/build/**" -ignore ".github/**" -failhard -q . 2>&1 | grep -v skipping
