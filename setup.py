import os, sys
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext

import os
assert "VERSION_STRING" in os.environ

with open("requirements.txt") as file:
    reqs = [z.strip() for z in file.readlines() if z.strip()]

# Taken from https://stackoverflow.com/questions/36841639/folder-for-data-files-in-setup-py-build-setuptools
# def generate_data_files():
#     data_files = []
#     for path, dirs, files in os.walk("cmake_install"):
#         install_dir = path[len("cmake_install/"):]
#         if len(files) > 0:
#             list_entry = (install_dir, [os.path.join(path, f) for f in files if not f.startswith('.')])
#             data_files.append(list_entry)

#     return data_files

class my_build_ext(build_ext):
    def build_extension(self, ext):
        # Do nothing
        pass

# Testing a fix for the stupid CI build failure
import fcntl; fcntl.fcntl(1, fcntl.F_SETFL, 0)


setup(
    name="zef",
    version=os.environ["VERSION_STRING"],
    author="Synchronous",
    packages=find_packages(),
    package_data={"zef": ["zeftypes_bootstrap*.json",
                          "pyzef*",
                          "libzef*",
                          "packaged_git_commit",
                          "auth.html"]},
    install_requires=reqs,
    # data_files=generate_data_files(),
    # python_requires=">=" + py_version
    # ext_package="pkg",
    ext_modules=[Extension('dontusethis', [])],
    cmdclass = {'build_ext': my_build_ext }
)
