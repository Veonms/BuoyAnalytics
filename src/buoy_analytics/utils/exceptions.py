import logging
import time
from functools import wraps


class NoDataRetrieved(Exception):
    """Exception raised when no data is returned. Inherits from Exception class."""

    pass


class SQLQueryExecutionFailed(Exception):
    """Exception raised when a SQL query could not be executed. Inherits from Exception class."""

    pass


def retry(
    ExceptionToCheck: Exception,
    tries: int = 3,
    delay: int = 3,
    backoff: int = 2,
):
    """Retries function {tries} number of tries.

    Args:
        ExceptionToCheck (Exception): Type of exception to check for.
        tries (int, optional): Number of retries. Defaults to 3.
        delay (int, optional): Length of delay (seconds). Defaults to 3.
        backoff (int, optional): Backoff multiplier. Defaults to 2.
    """

    def decorator_retry(func):
        @wraps(func)
        def func_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except ExceptionToCheck as err:
                    logging.warning(f"{err}. Retrying in {mdelay} seconds.")
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return func(*args, **kwargs)

        return func_retry

    return decorator_retry
