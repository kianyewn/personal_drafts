import json
import time

from decorators import log_function_call, timeit
from test_logging import logger


# @timeit
@log_function_call()
def test_func(a=2, b=3):
    logger.info("test_info")
    logger.error("test_error")
    logger.warning("test_warning")
    logger.debug("test_debug")
    logger.critical("test_critical")
    time.sleep(1)


def pretty_dump_json(data, file_path):
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
