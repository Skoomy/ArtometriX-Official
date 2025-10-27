"""
This module provides a decorator `timing` that can be used to measure and log
the execution time of functions or methods.
"""

import logging
import time
from collections.abc import Callable
from functools import wraps


def _func_full_name(func: Callable) -> str:
    """
    Helper function to get the full name of the function, including its module.

    Args:
        func (Callable): The function whose name is to be determined.

    Returns:
        str: Full name of the function in the format 'module.function'.
    """
    if not func.__module__:
        # If the function does not belong to a module, return only its qualified name
        return func.__qualname__

    # Return the function's name prefixed by its module
    return f"{func.__module__}.{func.__qualname__}"


def _human_readable_time(elapsed: float) -> str:
    """
    Convert elapsed time in seconds to a more human-readable format.

    Args:
        elapsed (float): Time elapsed in seconds.

    Returns:
        str: A human-readable string representing the elapsed time.
    """
    mins, secs = divmod(elapsed, 60)  # Convert seconds to minutes and seconds
    hours, mins = divmod(mins, 60)  # Convert minutes to hours and minutes

    if hours > 0:
        # Format time if elapsed time is more than an hour
        return f"{int(round(hours, 0))} hour {mins} min {round(secs, 2)} sec"
    elif mins > 0:
        # Format time if elapsed time is more than a minute but less than an hour
        return f"{int(round(mins, 0))} min {round(secs, 2)} sec"
    elif secs >= 0.1:
        # Format time if elapsed time is more than a tenth of a second but less than a minute
        return f"{round(secs, 2)} sec"
    else:
        # Format time in milliseconds if elapsed time is less than a tenth of a second
        return f"{int(round(secs * 1000.0, 0))} ms"


def timing(func: Callable) -> Callable:
    """
    Decorator that logs the execution time of the decorated function or method.

    Args:
        func (Callable): The function or method to decorate.

    Returns:
        Callable: The wrapped function with timing functionality.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Record the start time before calling the function
        t1 = time.perf_counter()

        # Call the actual function and store its result
        result = func(*args, **kwargs)

        # Calculate the elapsed time
        elapsed = time.perf_counter() - t1

        # Get the logger for the current module
        log = logging.getLogger(__name__)

        # Log the function's full name and its execution time in a human-readable format
        log.info(
            f"Run time: {_func_full_name(func)} ran in {_human_readable_time(elapsed)}"
        )

        # Return the result of the function call
        return result

    # Return the wrapped function
    return wrapper
