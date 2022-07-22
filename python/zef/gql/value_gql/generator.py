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

from ...core import *
from ...ops import *
import os


def generate_resolvers_file(schema_dict, resolvers_destination):
    """ Generates ariadne resolvers from the generator_base.py
        cog file. And outputs resolvers.py

        We pass a global dict with the schema dict that cog uses to generate the resolvers.
    """
    from cogapp import Cog
    import tempfile

    cog = Cog()
    cog.options.bReplace = True

    global_dict = {"schema_dict": schema_dict}

    try:
        path = os.path.dirname(os.path.realpath(__file__))
        cog.processFile(os.path.join(path, "generator_base.py"),
                        os.path.join(resolvers_destination, "resolvers.py"), globals=global_dict)
    except Exception as exc:
        print(f'An exception was raised when processing file {exc}')
