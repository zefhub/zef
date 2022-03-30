import os
from cogapp import Cog

def find_files_of_type(path, filename_endings, recurse=True, directories_to_exclude={}):
    found_files = set()
    def treat_dir(path):
        for entry in os.listdir(path):
            if not os.path.islink(entry):
                path_to_entry = path + '/' + entry
                if recurse and os.path.isdir(path_to_entry) and not any({os.path.samefile(path_to_entry, dir_) for dir_ in directories_to_exclude}):
                    treat_dir(path_to_entry)
                else:
                    if any({path_to_entry.endswith(ending) for ending in filename_endings}):
                        found_files.add(path_to_entry)
    treat_dir(path)
    return found_files

# two options to invoke:
# by inclusion:
#print(find_files_of_type(path='src_cpp', filename_endings={'.py', '.h', '.cpp'}))
#print("----------------")
#print(find_files_of_type(path='zefhub', filename_endings={'.py', '.h', '.cpp'}))
#print("----------------")
#print(find_files_of_type(path='zefdb', filename_endings={'.py', '.h', '.cpp'}))
#print("----------------")
#print(find_files_of_type(path='.', filename_endings={'.py', '.h', '.cpp'}, recurse=False))
# by exclusion:
#print(find_files_of_type(path='.', filename_endings={'.py', '.h', '.cpp'}, recurse=True, directories_to_exclude={'libraries'}))

cog = Cog()
cog.options.bReplace = False
cog.options.bDeleteCode = True
cog.options.sPrologue = """
import cog
import json
import os

et_filename = "zeftypes_ET.json" if os.path.exists('zeftypes_ET.json') else "zeftypes_bootstrap_ET.json"
with open(et_filename) as F:
    et = json.loads(F.read())

rt_filename = "zeftypes_RT.json" if os.path.exists('zeftypes_RT.json') else "zeftypes_bootstrap_RT.json"
with open(rt_filename) as F:
    rt = json.loads(F.read())

kw_filename = "zeftypes_KW.json" if os.path.exists('zeftypes_KW.json') else "zeftypes_bootstrap_KW.json"
with open(kw_filename) as F:
    kw = json.loads(F.read())

en_filename = "zeftypes_EN.json" if os.path.exists('zeftypes_EN.json') else "zeftypes_bootstrap_EN.json"
with open(en_filename) as F:
    en = json.loads(F.read())

def enum_type(x):
    return x.split('.')[0]
def enum_val(x):
    return x.split('.')[1]
"""
#cog.options.verbosity = 0
for filename in find_files_of_type(path='src_cpp', filename_endings={'.cog'}, recurse=True):#, directories_to_exclude={'libraries','src_cpp/build_julia_package','gql'}):
    try:
        true_output = filename[:-len(".cog")] + ".gen"
        cog.options.sOutputName = true_output + ".tmp"
        cog.processOneFile(filename)
        if not os.path.exists(true_output) or open(true_output + ".tmp").read() != open(true_output).read():
            print(filename, " changed")
            os.rename(true_output + ".tmp", true_output)
        else:
            os.unlink(true_output + ".tmp")
    except Exception as exc:
        print(f'An exception was raised when processing file "{filename}": {exc}')
        # Need this to fail for cmake to not continue on without a care.
        raise

for filename in find_files_of_type(path='zef', filename_endings={'.cog'}, recurse=True):
    try:
        true_output = filename[:-len(".cog")]
        cog.options.sOutputName = true_output + ".tmp"
        cog.processOneFile(filename)
        if not os.path.exists(true_output) or open(true_output + ".tmp").read() != open(true_output).read():
            print(filename, " changed")
            os.rename(true_output + ".tmp", true_output)
        else:
            os.unlink(true_output + ".tmp")
    except Exception as exc:
        print(f'An exception was raised when processing file "{filename}": {exc}')
        # Need this to fail for cmake to not continue on without a care.
        raise
