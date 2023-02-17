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

from robot.api.deco import keyword, library
from robot.api import SkipExecution, Failure, Error, FatalError
import robot.libraries.BuiltIn
BuiltIn = robot.libraries.BuiltIn.BuiltIn()

from subprocess import Popen, PIPE, DEVNULL
import os, sys

from dataclasses import dataclass

@dataclass
class REPL:
    proc: Popen
    to_repl_read_fd: int
    to_repl_write_fd: int
    from_repl_read_fd: int
    from_repl_write_fd: int
    from_file: any
    to_file: any
    temp_stderr: any
    temp_stdout: any

    last_result: str = None
    last_dt: float = None

    need_wait: bool = False

# TODO: Need to fix the problem when overflowing the stdout/err of a child
# process. And do something better than DEVNULL.

@library
class REPLRunner:
    ROBOT_LIBRARY_SCOPE = "TEST" # "SUITE" and "GLOBAL" are options
    ROBOT_LISTENER_API_VERSION = 2


    def __init__(self):
        self.repls = []
        self.ROBOT_LIBRARY_LISTENER = self

    def _start_test(self, name, attrs):
        BuiltIn.set_test_variable("${ACTIVE_REPL}", None)

    def _end_test(self, name, attrs):
        for i,repl in enumerate(self.repls):
            if repl is None:
                continue
            if repl.proc.returncode is None:
                self.quit_repl(i)

    def _get_active_repl(self):
        return BuiltIn.replace_variables("${ACTIVE_REPL}")

    def _set_last(self, i, dt, result):
        self.repls[i].last_dt = dt
        self.repls[i].last_result = result

    @keyword
    def custom_library(self):
        return 42

    def check_not_failed(self, i):
        if not self.repls[i].from_file.closed and not self.repls[i].to_file.closed:
            if self.repls[i].proc.returncode is None:
                return True
            if self.repls[i].proc.returncode == 0:
                return True
        self.report_errors(i)
        return False

    def get_stderrout(self, i):
        # self.repls[i].temp_stderr.seek(0)
        stderr = self.repls[i].temp_stderr.read().decode()
        # self.repls[i].temp_stdout.seek(0)
        stdout = self.repls[i].temp_stdout.read().decode()

        return stderr,stdout

    def report_errors(self, i):
        print(f"*ERROR* The REPL index {i} failed. stderr was:")
        stderr,stdout = self.get_stderrout(i)
        print(stderr)
        print(f"*ERROR* stdout was:")
        print(stdout)

    @keyword
    def start_repl(self):
        script_location = os.path.join(os.path.dirname(__file__), "repl_script.py")

        to_repl_read_fd,to_repl_write_fd = os.pipe()
        from_repl_read_fd,from_repl_write_fd = os.pipe()

        import tempfile
        temp_stdout = tempfile.NamedTemporaryFile(prefix="zefrobot_", delete=False)
        temp_stderr = tempfile.NamedTemporaryFile(prefix="zefrobot_", delete=False)
        print("temp_stdout:", temp_stdout.name)
        print("temp_stderr:", temp_stderr.name)

        proc = Popen([sys.executable, "-u", script_location, str(to_repl_read_fd), str(from_repl_write_fd)],
                     # stdout=PIPE, stderr=PIPE,
                     # stdout=DEVNULL, stderr=DEVNULL,
                     stdout=temp_stdout, stderr=temp_stderr,
                     pass_fds=[to_repl_read_fd, from_repl_write_fd])
        os.close(to_repl_read_fd)
        os.close(from_repl_write_fd)

        from_file = os.fdopen(from_repl_read_fd, "rb", 0)
        to_file = os.fdopen(to_repl_write_fd, "wb", 0)

        self.repls += [REPL(proc, to_repl_read_fd, to_repl_write_fd, from_repl_read_fd, from_repl_write_fd, from_file, to_file, temp_stderr, temp_stdout)]

        i = len(self.repls) - 1
        BuiltIn.set_test_variable("${ACTIVE_REPL}", i)

        line = self.repls[i].from_file.readline()
        line = line.strip()
        # TODO: Make this delayed
        if line != b"START_SUCCESS":
            self.repls[i] = None
            raise Exception("Failed to start REPL properly")

        return i

    @keyword
    def get_all_repls(self):
        return list(range(len(self.repls)))

    # @keyword
    # def send_repl(self, arg1, arg2=None):
    #     if arg2 is None:
    #         cmd = arg1
    #         i = self._get_active_repl()
    #     else:
    #         i,cmd = arg1,arg2

    #     assert self.check_not_failed(i)
    #     print(f"*INFO* running command {cmd}")
    #     try:
    #         self.repls[i].to_file.write(("EXEC " + cmd + "\n").encode())
    #         line = self.repls[i].from_file.readline()
    #         line = line.strip()
    #         if not line.startswith(b"SUCCESS "):
    #             raise Failure("Was a failure: '" + line.decode() + "'")

    #         line = line[len(b"SUCCESS "):].decode()
    #         dt = float(line)
    #         self._set_last(i, dt, None)
    #     except BrokenPipeError:
    #         print("*ERROR* Send REPL failed")
    #         self.report_errors(i)
    #         raise

    @keyword
    def send_repl(self, arg1, arg2=None):
        if arg2 is None:
            cmd = arg1
            i = self._get_active_repl()
        else:
            i,cmd = arg1,arg2

        self.send_repl_no_wait(i, cmd)
        self.wait_repl(i)
    

    @keyword
    def send_repl_no_wait(self, arg1, arg2=None):
        if arg2 is None:
            cmd = arg1
            i = self._get_active_repl()
        else:
            i,cmd = arg1,arg2

        if self.repls[i].need_wait:
            raise Error("Didn't wait after a Send Repl")

        assert self.check_not_failed(i)
        print(f"*INFO* running command {cmd}")
        try:
            self.repls[i].to_file.write(("EXEC " + cmd + "\n").encode())
        except BrokenPipeError:
            print("*ERROR* Send REPL failed")
            self.report_errors(i)
            raise

        self.repls[i].need_wait = True

    @keyword
    def wait_repl(self, i=None):
        if i is None:
            i = self._get_active_repl()

        if not self.repls[i].need_wait:
            raise Error("Trying to wait without first doing a Send Repl")

        assert self.check_not_failed(i)
        try:
            line = self.repls[i].from_file.readline()
            line = line.strip()
            if not line.startswith(b"SUCCESS "):
                # stderr,stdout = self.get_stderrout(i)
                # print("{stderr}\n{stdout}")
                print(f"temp_stderr: {self.repls[i].temp_stderr.name}, temp_stdout: {self.repls[i].temp_stdout.name}")
                raise Failure("Was a failure: '" + line.decode() + "'")

            line = line[len(b"SUCCESS "):].decode()
            dt = float(line)
            self._set_last(i, dt, None)
        except BrokenPipeError:
            print("*ERROR* Send REPL failed")
            self.report_errors(i)
            raise

        self.repls[i].need_wait = False

    @keyword
    def eval_repl(self, arg1, arg2=None):
        if arg2 is None:
            cmd = arg1
            i = self._get_active_repl()
        else:
            i,cmd = arg1,arg2

        assert self.check_not_failed(i)
        try:
            self.repls[i].to_file.write(("EVAL " + cmd + "\n").encode())
            line = self.repls[i].from_file.readline()
            line = line.strip()
            if not line.startswith(b"RESPONSE"):
                # stderr,stdout = self.get_stderrout(i)
                # print("{stderr}\n{stdout}")
                print(f"temp_stderr: {self.repls[i].temp_stderr.name}, temp_stdout: {self.repls[i].temp_stdout.name}")
                raise Failure(f"*ERROR* Eval REPL of {cmd}\ngave a failure: '{line.decode()}'")
            line = line[len(b"RESPONSE "):].decode()
            dt,result = line.split(" ", maxsplit=1)
            dt = float(dt)
            self._set_last(i, dt, result)

            return result
        except BrokenPipeError:
            print("*ERROR* Eval REPL failed")
            self.report_errors(i)
            raise

    @keyword
    def quit_repl(self, i=None):
        if i is None:
            i = self._get_active_repl()
            
        assert self.check_not_failed(i)
        try:
            self.repls[i].to_file.write(b"EXIT")
            self.repls[i].to_file.close()
            # self.repls[i].proc.wait()
        except:
            print("*ERROR* Quit REPL failed")
            self.report_errors(i)
            raise

    @keyword
    def get_last_dt(self, i=None):
        if i is None:
            i = self._get_active_repl()
        return self.repls[i].last_dt

    @keyword
    def get_last_result(self, i=None):
        if i is None:
            i = self._get_active_repl()
        return self.repls[i].last_result

    @keyword
    def switch_repl(self, i: int):
        assert 0 <= i < len(self.repls)
        BuiltIn.set_test_variable("${ACTIVE_REPL}", i)