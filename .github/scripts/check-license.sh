#!/bin/bash

addlicense -c "Synchronous Technologies Pte Ltd" -f ../assets/apache-license.md -ignore "_cmake_deps/**" -ignore "cmake_build/**" -ignore ".github/**" -failhard .
