import sys
assert len(sys.argv) == 2

wheel_file = sys.argv[1]
with open(wheel_file) as file:
    wheels = file.readlines()

wheels = [wheel.strip() for wheel in wheels if len(wheel.strip()) >= 1]

if len(wheels) == 0:
    raise Exception("No wheels found in s3!")

import os

with open("index.html", "w") as file:
    file.write("""
    <html>
<body>
<table>
    <tr>
    <th>Link</th>
    <th>Platform</th>
    <th>Python version</th>
    </tr>
    """)

    for wheel in wheels:
        if "linux" in wheel:
            platform = "Linux"
        elif "macos" in wheel:
            platform = "MacOS"
        else:
            platform = "Unknown"

        import re
        try:
            m = re.search(r"-cp3([^-]*)-", wheel)
            py_version = f"3.{m[1]}"
        except:
            print(f"Issue with {wheel} and {m} for py_version")
            py_version = "error"
        try:
            m = re.search(r"zefdb-([^-]*)-", wheel)
            zefdb_version = m[1]
        except:
            print(f"Issue with {wheel} and {m} for zefdb_version")
            zefdb_version = "error"
        file.write(f"""
<tr>
<td><a href="{wheel}">{wheel}</a></td>
<td>{platform}</td>
<td>{py_version}</td>
</tr>""")

    file.write("""
</table>
    </body>
    </html>""")
