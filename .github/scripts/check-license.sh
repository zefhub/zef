#!/bin/bash

addlicense -v -c "Synchronous Technologies Pte Ltd" -f .github/assets/apache-license.md -ignore "core/build/**" -ignore "python/pyzef/build/**" -ignore ".github/**" -ignore "addlicense.sh" -failhard -q . 2>&1
