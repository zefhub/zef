# Installing from PyPI

Simply run:

`pip install zef`

You may have to replace `pip` with `pip3` in some distributions.

Native Windows installs are currently not supported, although it is possible to
install Zef in a WSL (Windows Subsystem for Linux) environment. We are aiming to
include native Windows support soon.

# Compiling from source

Either obtain the source distribution from PyPI using:

`pip install --no-binary zef zef`

or obtain the source from the git repo using:

`git clone https://github.com/zefhub/zef`

and then

`pip install zef/python`

# Requirements

Compiling from source requires the following system libraries:

- OpenSSL and headers

and the following build-time python libraries:

- pybind11
- cogapp
- pyfunctional


# For Developers

A convenience script exists in the repo root:

`bash compile_for_local_dev.sh`

which compiles both libzef and the python bindings, along with including a symlink to the library in the source repo. Adding the `<repo_root>/python` path to `PYTHONPATH` will then allow `import zef` to find the package.

You may need to manually include all of the python requirements for Zef by
running:

`pip install -r python/requirements.txt`
