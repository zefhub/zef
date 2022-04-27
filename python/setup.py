import os, sys
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.sdist import sdist

import configparser

import subprocess

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
                # Hint CMake to use the same Python executable that
                # is launching the build, prevents possible mismatching if
                # multiple versions of Python are installed
                '-DPython3_EXECUTABLE={}'.format(sys.executable),
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
            
            


class override_sdist(sdist):
    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)

        print("* Adding libzef to sdist")

        # Adding in the libzef core too, using git to obtain the list of files.
        target_dir = os.path.join(base_dir, "libzef")
        print("Target dir is", target_dir)
        # if os.path.exists(target
        os.makedirs(target_dir)
        print("** Copying all of the files foudn with git ls-files across using tar packing/unpacking")
        filelist = subprocess.Popen(["git", "ls-files", "."], cwd="../core", stdout=subprocess.PIPE)
        tarcreate = subprocess.Popen(["tar", "-cvf", "-", "-T", "-"], cwd="../core", stdin=filelist.stdout, stdout=subprocess.PIPE)
        subprocess.check_call(["tar", "-xvf", "-"], cwd=target_dir, stdin=tarcreate.stdout)

        print("** Adding in the get_zeftypes.py script manually")
        # We need to include the get_zeftypes.py file along with the distribution
        import shutil
        shutil.copy("../scripts/get_zeftypes.py", os.path.join(base_dir, "libzef", "scripts"))

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




# I HATE PYTHON PACKAGING
sys.path += [""]
import versioneer

with open("requirements.txt") as file:
    reqs = [z.strip() for z in file.readlines() if z.strip()]


setup(
    name="zef",
    author="ZefHub.io",
    python_requires=">=3.8",
    version=versioneer.get_version(),
    packages=find_packages(),
    install_requires=reqs,
    ext_modules=[pyzef_ext],
    cmdclass = versioneer.get_cmdclass({
        'build_ext': cmake_build_ext,
        'sdist': override_sdist,
    })
)


