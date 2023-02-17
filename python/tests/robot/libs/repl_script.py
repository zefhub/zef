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

import sys
import os
import time

def wait_and_execute(file_in, file_out):
    print("Start of wait")
    globs = {}
    exec("from zef import *", globs, globs)
    exec("from zef.ops import *", globs, globs)
    exec("import zef", globs, globs)

    try:
        exec("""zef.core.internals.wait_for_auth()""", globs, globs)
    except:
        file_out.write("START_FAILURE\n".encode())
    file_out.write("START_SUCCESS\n".encode())

    while True:
        print("Waiting for a line")
        line = file_in.readline()
        line = line.strip()

        print(f"Read in line: {line}")
        line = line.decode()

        if line.startswith("EXIT"):
            break
        elif line.startswith("EXEC"):
            try:
                start = time.time()
                exec(line[5:], globs, globs)
                dt = time.time() - start
                print("Should be writing success")
                file_out.write((f"SUCCESS {dt}\n").encode())
            except Exception as exc:
                print("Should be writing failure")
                import traceback ; traceback.print_exc()
                file_out.write(("FAILURE " + str(exc) + "\n").encode())
        elif line.startswith("EVAL"):
            try:
                start = time.time()
                out = eval(line[5:], globs, globs)
                dt = time.time() - start
                file_out.write((f"RESPONSE {dt} {out}\n").encode())
            except Exception as exc:
                file_out.write(("FAILURE " + str(exc) + "\n").encode())
        else:
            raise Exception("Unknown command: " + line)


if __name__ == "__main__":
    _,fd_in,fd_out = sys.argv
    fd_in = int(fd_in)
    fd_out = int(fd_out)
    try:
        file_in = os.fdopen(fd_in, "rb", 0)
        file_out = os.fdopen(fd_out, "wb", 0)
        wait_and_execute(file_in, file_out)
    except:
        os.close(fd_out)
        os.close(fd_in)
        raise