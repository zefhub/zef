import structlog

# Trying to keep a config that doesn't interfer with any user's global settings
# for structlog if they happen to use it.
log = structlog.wrap_logger(None,
    processors=[
        # structlog.stdlib.filter_by_level,
        # structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        # structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        # The following only does anything if exc_info is passed as a keyword
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(colors=True),
    ]
)
