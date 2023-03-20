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


def c_schema_validator(ctx):
    """Don't call this explicitly. Only for the zefdb core."""

    # We can move this to another section later on
    from .. import Graph
    from .. import _ops as zo, internals
    from ...pyzef import zefops as pyzo
    from ...pyzef.internals import AbortTransaction
    from .. import RT

    try:
        root_node = pyzo.now(Graph(ctx)[internals.root_node_blob_index()])
        schemas = pyzo.traverse_out_node_multi(root_node, internals.RT.ZEF_Schema)
        for schema in schemas:
            # This stays python-style as we will will only get here when a schema is present.
            lt = schema | zo.value | zo.collect
            if {"graph_slice": gs} | zo.Not[zo.is_a[lt]] | zo.collect:
                raise Exception(f"{schema} failed to validate graph slice.")
    except Exception as exc:
        from ..logger import log
        log.error("There was an error in c_schema_validator", exc_info=exc)
        print(str(exc))
        raise

def register_schema_validator():
    from ...pyzef import internals
    internals.register_schema_validator(c_schema_validator)
