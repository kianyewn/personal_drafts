import cProfile
import pstats
import time
from datetime import datetime
from functools import wraps

from loguru import logger


# Decorator for timing functions
def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        # first item in the args, ie `args[0]` is `self`
        logger.info(
            f"Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds"
        )
        return result

    return timeit_wrapper


def log_function_call(func_name: str = None):
    """Decorator to log function calls with parameters for debugging"""

    def decorator(func):
        import functools

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = func_name or func.__name__
            logger.opt(depth=1).debug(
                f"Calling {name}",
                extra={
                    "function": name,
                    "args_count": len(args),
                    "kwargs_keys": list(kwargs.keys()),
                    "module": func.__module__,
                },
            )

            try:
                start_time = datetime.now()
                result = func(*args, **kwargs)
                execution_time = (datetime.now() - start_time).total_seconds()

                logger.debug(
                    f"Completed {name}",
                    extra={
                        "function": name,
                        "execution_time": execution_time,
                        "success": True,
                    },
                )

                return result

            except Exception as e:
                execution_time = (datetime.now() - start_time).total_seconds()

                logger.error(
                    f"Failed {name}: {str(e)}",
                    extra={
                        "function": name,
                        "execution_time": execution_time,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "success": False,
                    },
                    exc_info=True,
                )
                raise

        return wrapper

    return decorator


def profile_function(func):
    """Profile function performance"""

    def wrapper(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        result = func(*args, **kwargs)
        profiler.disable()

        # Save stats
        stats = pstats.Stats(profiler)
        stats.sort_stats("cumulative")
        stats.print_stats(20)  # Top 20 functions

        return result

    return wrapper
