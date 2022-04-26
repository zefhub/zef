import os, sys
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext

import configparser

import subprocess

with open("requirements.txt") as file:
    reqs = [z.strip() for z in file.readlines() if z.strip()]



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
            cmake_args = [
                f'-DCMAKE_BUILD_TYPE={cfg}',
                # Ask CMake to place the resulting library in the directory
                # containing the extension
                # f'-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}',
                # Other intermediate static libraries are placed in a
                # temporary build directory instead
                # f'-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY={self.build_temp}',
                # Hint CMake to use the same Python executable that
                # is launching the build, prevents possible mismatching if
                # multiple versions of Python are installed
                # '-DPYTHON_EXECUTABLE={}'.format(sys.executable),
                # Add other project-specific CMake arguments if needed
                # ...
                f"-DCMAKE_PREFIX_PATH={cmake_path_s}",
                # f"-DPython_ROOT_DIR={sys.base_prefix}",

                f"-DCMAKE_INSTALL_PREFIX={extdir}",
            ] + ext.cmake_args

            # Config
            subprocess.check_call(['cmake', ext.cmake_lists_dir] + cmake_args,
                                  cwd=this_build_dir)

            # Build
            subprocess.check_call(['cmake', '--build', '.', '--config', cfg, '--', '-j'],
                                  cwd=this_build_dir)

            # Install to the lib dir
            subprocess.check_call(['cmake', '--install', '.'],
                                  cwd=this_build_dir)
            
            


root = os.getcwd()
setup_cfg = os.path.join(root, "setup.cfg")
parser = configparser.ConfigParser()
with open(setup_cfg, "r") as cfg_file:
    parser.read_file(cfg_file)
libzef_location = parser.get("libzef", "location", fallback=None)
libzef_kind = parser.get("libzef", "kind", fallback="guess")
if libzef_kind == "guess":
    if libzef_location is None:
        # We first try without the default location to see if anything is found
        ret = subprocess.call(['cmake', '-P', 'lookforzef.cmake'])
        if ret == 0:
            libzef_kind = "binary"
        else:
            libzef_location = "../core"

    if libzef_kind == "guess":
        ret = subprocess.call(['cmake', '-DCMAKE_PREFIX_PATH=' + libzef_location, '-P', 'lookforzef.cmake'])
        if ret == 0:
            libzef_kind = "binary"
        elif os.path.exists(os.path.join(libzef_location, "zefDBConfig.cmake.in")):
            libzef_kind = "source"
        else:
            raise Exception(f"Can't guess what kind of libzef exists at '{libzef_location}'")
    
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


import versioneer

if __name__ == "__main__":
    setup(
        name="zef",
        version=versioneer.get_version(),
        author="ZefHub.io",
        packages=find_packages(),
        install_requires=reqs,
        ext_modules=[pyzef_ext],
        cmdclass = versioneer.get_cmdclass({'build_ext': cmake_build_ext})
    )


