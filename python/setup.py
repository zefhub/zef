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

import os, sys
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.sdist import sdist

import configparser

import subprocess

sys.path += [""]
import versioneer
versioneer_version = versioneer.get_version()

# This setup has been stolen and merged from a few places... it really needs to
# be fixed up.
class CMakeExtension(Extension):
    def __init__(self, name, cmake_lists_dir='.', cmake_path=[], cmake_args=[], **kwa):
        Extension.__init__(self, name, sources=[], **kwa)
        self.cmake_lists_dir = os.path.abspath(cmake_lists_dir)
        self.cmake_path = cmake_path
        self.cmake_args = cmake_args

class cmake_build_ext(build_ext):
    def build_extensions(self):
        # Ensure that CMake is present and working
        try:
            out = subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError('Cannot find CMake executable')

        for ext in self.extensions:

            extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
            cfg = 'Release'

            this_build_dir = os.path.join(self.build_temp, ext.name)
            # this_install_dir = os.path.join(self.build_temp, ext.name + "_install")

            os.makedirs(this_build_dir, exist_ok=True)
            # os.makedirs(this_install_dir, exist_ok=True)

            cmake_path_s = ':'.join(os.path.abspath(x) for x in ext.cmake_path)
            # Use ninja if available
            if sys.platform == 'win32':
                ret = 1
            else:
                ret = subprocess.call(['which', 'ninja'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if ret == 0:
                cmake_env = {"CMAKE_GENERATOR": "Ninja"}
                # The update after the initial value will allow an external caller to override this option if they want.
                cmake_env.update(os.environ)
            else:
                cmake_env = None
            cmake_args = [
                f'-DCMAKE_BUILD_TYPE={cfg}',
                # Hint CMake to use the same Python executable that
                # is launching the build, prevents possible mismatching if
                # multiple versions of Python are installed
                '-DPython3_EXECUTABLE={}'.format(sys.executable),
                # Add other project-specific CMake arguments if needed
                # ...
                f"-DCMAKE_PREFIX_PATH={cmake_path_s}",
                # f"-DPython_ROOT_DIR={sys.base_prefix}",

                f"-DCMAKE_INSTALL_PREFIX={extdir}",
                "-DCMAKE_OSX_DEPLOYMENT_TARGET=10.15",
                f"-DLIBZEF_PACKAGE_VERSION={versioneer_version}",
            ] + ext.cmake_args

            # For debugging CI builds
            # subprocess.call(['set'], shell=True, env=cmake_env)

            # Config
            subprocess.check_call(['cmake', ext.cmake_lists_dir] + cmake_args,
                                  cwd=this_build_dir, env=cmake_env)

            # Build
            subprocess.check_call(['cmake', '--build', '.', '--config', cfg, '-v'],
                                  cwd=this_build_dir, env=cmake_env)

            # Install to the lib dir
            subprocess.check_call(['cmake', '--install', '.'],
                                  cwd=this_build_dir, env=cmake_env)
            
            


class override_sdist(sdist):
    def make_release_tree(self, base_dir, files):
        import shutil

        sdist.make_release_tree(self, base_dir, files)

        print("* Adding license and readme to sdist")
        shutil.copy("../LICENSE", os.path.join(base_dir, "LICENSE"))
        shutil.copy("../README.md", os.path.join(base_dir, "README.md"))

        print("* Adding libzef to sdist")

        # Adding in the libzef core too, using git to obtain the list of files.
        target_dir = os.path.join(base_dir, "libzef")
        print("Target dir is", target_dir)
        # if os.path.exists(target
        os.makedirs(target_dir)
        print("** Copying all of the files found with git ls-files across using tar packing/unpacking")
        filelist = subprocess.Popen(["git", "ls-files", "."], cwd="../core", stdout=subprocess.PIPE)
        tarcreate = subprocess.Popen(["tar", "-cvf", "-", "-T", "-"], cwd="../core", stdin=filelist.stdout, stdout=subprocess.PIPE)
        subprocess.check_call(["tar", "-xvf", "-"], cwd=target_dir, stdin=tarcreate.stdout)

        print("** Adding in the get_zeftypes.py script manually")
        # We need to include the get_zeftypes.py file along with the distribution
        shutil.copy("../scripts/get_zeftypes.py", os.path.join(target_dir, "scripts"))

        print("** Adding in the LICENSE file")
        shutil.copy("../LICENSE", target_dir)

        print("** Updating the setup.cfg file")
        # We now overwrite the setup.cfg file to point at this new directory
        setup_cfg = os.path.join(base_dir, "setup.cfg")
        parser = configparser.ConfigParser()
        with open(setup_cfg) as cfg_file:
            parser.read_file(cfg_file)
        parser["libzef"]["location"] = "libzef"
        parser["libzef"]["kind"] = "source"
        with open(setup_cfg, "w") as cfg_file:
            parser.write(cfg_file)





# This here is to create the declaration of the extension used in th build.
# Unfortunately it is run, and is very spammy, even if this file is used for
# other commands like sdist.

root = os.getcwd()
setup_cfg = os.path.join(root, "setup.cfg")
parser = configparser.ConfigParser()
with open(setup_cfg, "r") as cfg_file:
    parser.read_file(cfg_file)
libzef_location = parser.get("libzef", "location", fallback=None)
if libzef_location is None:
    libzef_location = os.environ.get("LIBZEF_LOCATION", None)
libzef_kind = parser.get("libzef", "kind", fallback="guess")
if libzef_kind == "guess":
    if libzef_location is None:
        # We first try without the default location to see if anything is found
        ret = subprocess.call(['cmake', '-P', 'lookforzef.cmake'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if ret == 0:
            libzef_kind = "binary"
        else:
            libzef_location = "../core"

    if libzef_kind == "guess":
        ret = subprocess.call(['cmake', '-DCMAKE_PREFIX_PATH=' + libzef_location, '-P', 'lookforzef.cmake'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if ret == 0:
            libzef_kind = "binary"
        elif os.path.exists(os.path.join(libzef_location, "zefDBConfig.cmake.in")):
            libzef_kind = "source"
        else:
            raise Exception(f"Can't guess what kind of libzef exists at '{libzef_location}' looking from the cwd of '{os.getcwd()}'. If this is a relative directory, and you are installing via `pip wheel ...` or similar, then you should specify it in either the setup.cfg, or via the `LIBZEF_LOCATION` environment variable, as a absolute directory.")

    print(f"libzef location was guessed to be '{libzef_kind}' from using directory '{libzef_location}' (although it may be on the cmake path instead)")
    
if libzef_location is None:
    libzef_paths = []
else:
    libzef_paths = [libzef_location]

if libzef_kind == "binary":
    print(f"Using libzef as a binary with locations: {libzef_paths}")
    pyzef_ext = CMakeExtension("zef.pyzef", "pyzef",
                    cmake_path=libzef_paths,)
elif libzef_kind == "source":
    print(f"Bundling libzef from source in locations: {libzef_paths}")
    pyzef_ext = CMakeExtension("zef.pyzef", "pyzef",
                    cmake_path=libzef_paths,
                    cmake_args=["-DLIBZEF_PYZEF_BUNDLED=TRUE"])
else:
    raise Exception(f"Don't understand libzef_kind of '{libzef_kind}'")


if os.path.exists("README.md"):
    with open("README.md", "rb") as file:
        long_desc = file.read().decode("utf-8")
else:
    with open("../README.md", "rb") as file:
        long_desc = file.read().decode("utf-8")

with open("requirements.txt") as file:
    reqs = [z.strip() for z in file.readlines() if z.strip()]


setup(
    long_description=long_desc,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    version=versioneer_version,
    packages=find_packages(),
    install_requires=reqs,
    ext_modules=[pyzef_ext],
    cmdclass = versioneer.get_cmdclass({
        'build_ext': cmake_build_ext,
        'sdist': override_sdist,
    })
)


