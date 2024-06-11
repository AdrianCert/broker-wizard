import logging

lib_logger = logging.getLogger("broker_wizard")


def setup_logging(level=logging.INFO):
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )  
    stream_handler.setFormatter(formatter)

    lib_logger.addHandler(stream_handler)
    lib_logger.setLevel(level)

debug = lib_logger.debug
info = lib_logger.info
warning = lib_logger.warning
error = lib_logger.error
critical = lib_logger.critical
exception = lib_logger.exception
log = lib_logger.log

setup_logging()

__all__ = [
    "debug",
    "info",
    "warning",
    "error",
    "critical",
    "exception",
    "log",
]
