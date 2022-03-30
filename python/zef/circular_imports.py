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

# Heavily modified from https://stackoverflow.com/a/2406043

import types
# Getting really frustrated with circular imports
beingimported = []
originalimport = __import__
def newimport(modulename, globs=None, locals=None, fromlist=(), level=0):
    if isinstance(globs, dict):
        parent_name = globs.get("__name__", None)
        is_package = (parent_name == globs.get("__package__", "1"))
    else:
        parent_name = ""
        is_package = False
    if level == 0:
        qual_name = modulename
    else:
        rel_level = level
        if is_package:
            rel_level -= 1
        if rel_level == 0:
            rel_name = parent_name
        else:
            rel_name = '.'.join(parent_name.split('.')[:-rel_level])
        if modulename == "":
            qual_name = rel_name
        else:
            qual_name = rel_name + '.' + modulename

    # Trying method with all fromlist used instead
    was_already_present = qual_name in beingimported
    beingimported.append(qual_name)

    sub_check = {}
    if isinstance(fromlist, tuple):
        for item in fromlist:
            if item == "*":
                continue
            sub_qual_name = qual_name + "." + item
            sub_check[item] = (sub_qual_name, sub_qual_name in beingimported)
            beingimported.append(sub_qual_name)

    try:
        result = originalimport(modulename, globs, locals, fromlist, level)

        try:
            if "zef" in parent_name:
                # only_importing_submodules = isinstance(fromlist, tuple) and ('*' not in fromlist) and all(isinstance(getattr(result, x), types.ModuleType) for x in fromlist)
                # except Exception as exc:
                #     print(f"Errored: {exc}")
                #     print(dir(result))
                #     only_importing_submodules = False
                only_importing_submodules = (len(sub_check) > 0)
                for sub_name, (sub_qual_name, sub_was_previous) in sub_check.items():
                    try:
                        sub_obj = getattr(result, sub_name)
                    except:
                        print(f"!!! After trying to import item {sub_qual_name=} it wasn't in the module!")
                        raise
                    if isinstance(sub_obj, types.ModuleType):
                        if sub_was_previous:
                            import traceback
                            frame = traceback.extract_stack()[-2]
                            print("=========")
                            print(f"Importing in circles {sub_qual_name=} from import {qual_name=} {level=} {parent_name=} {is_package=} {fromlist=}")
                            print("    Currently importing -> ", beingimported)
                            print_all_imports_in_stack()
                    else:
                        only_importing_submodules = False
                if was_already_present and not only_importing_submodules:
                    # We don't worry about importing submodules from us when we're a package
                    is_submodule_import = (is_package and level == 1)
                    if not is_submodule_import:
                        import traceback
                        frame = traceback.extract_stack()[-2]
                        print("=========")
                        print(f"Importing in circles {modulename=} {qual_name=} {level=} {parent_name=} {is_package=} {fromlist=}")
                        print("    Currently importing -> ", beingimported)
                        print_all_imports_in_stack()
        except Exception as exc:
            print("Ignoring exc in detecting circular imports")
    finally:
        def remove_last_occurrence(l, x):
            if x in l:
                l.reverse()
                l.remove(x)
                l.reverse()
        remove_last_occurrence(beingimported, qual_name)
        for (sub_qual_name, sub_was_previous) in sub_check.values():
            remove_last_occurrence(beingimported, sub_qual_name)

    return result

def check_circular_imports():
    import builtins
    builtins.__import__ = newimport

def disable_check_circular_imports():
    import builtins
    builtins.__import__ = originalimport



def print_all_imports_in_stack():
    import traceback
    stack = traceback.extract_stack()[:-2]
    stack.reverse()

    for frame in stack:
        if "import" in frame.line and 'circular_imports' not in frame.filename:
            print(f"   {frame.filename}: {frame.lineno} -- {frame.line}")
            
    