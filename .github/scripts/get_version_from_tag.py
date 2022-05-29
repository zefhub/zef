import subprocess
import sys

out = subprocess.check_output(["git", "tag", "--points-at", "HEAD"])

tags = out.split()

if len(tags) == 0:
    print("No tags found")
    sys.exit(1)

# Get rid of the 'pyzef' in front
tags = [x[len('pyzef-'):] for x in tags if x.startswith('pyzef-')]

from packaging import version
# Get rid of any alpha/dev versions
tags = [x for x in tags if not version.parse(x).is_prerelease]
tags = [x for x in tags if not version.parse(x).is_devrelease]

if len(tags) == 0:
    print("No release tags found")
    sys.exit(1)


tags.sort(key=version.parse, reverse=True)
print(tags[0])
sys.exit(0)


    