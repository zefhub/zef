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

import structlog

# Trying to keep a config that doesn't interfer with any user's global settings
# for structlog if they happen to use it.
import os
logger_type = os.environ.get("ZEFDB_LOGGER", "RICH").upper()
if logger_type not in ["RICH","PLAIN"]:
    print(f"Did not understand ZEFDB_LOGGER type of {logger_type}, going to use PLAIN by default.")
    logger_type = "PLAIN"
if logger_type == "RICH":
    log = structlog.wrap_logger(None,
        processors=[
            # structlog.stdlib.filter_by_level,
            # structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            # structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            # The following only does anything if exc_info is passed as a keyword
            # structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    )
elif logger_type == "PLAIN":
    log = structlog.wrap_logger(None,
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            # The following only does anything if exc_info is passed as a keyword
            # structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            # structlog.processors.KeyValueRenderer(),
            structlog.dev.ConsoleRenderer(colors=False, exception_formatter=structlog.dev.plain_traceback),
        ]
    )
else:
    raise Exception("Should never get here")