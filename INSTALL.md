# Installing from PyPI

Simply run:

`pip install zef`

You may have to replace `pip` with `pip3` in some distributions.

Native Windows installs are currently not supported, although it is possible to
install Zef in a WSL (Windows Subsystem for Linux) environment. We are aiming to
include native Windows support soon.

# Compiling from source

You can obtain the source distribution from PyPI for a release using:

`pip install --no-binary zef zef`

If you want to compile from the latest of a branch from the GitHub repo, then
you have two choices:

1. Use a system-installed libzef. This is currently not possible.
2. Use a bundled lizef. In order to do this you should make an sdist package
yourself. This is because of the way that pip and friends find files to copy.
Hence:

```
git clone https://github.com/zefhub/zef
cd zef/python
python setup.py sdist
pip install dist/zef-<version>.tar.gz
```

You can look at the `dockerfiles/Dockerfile.compat` as an example of first
building a sdist package then installing it.

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
