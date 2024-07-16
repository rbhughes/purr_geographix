import asyncio
import functools
import hashlib
import json
import pandas as pd
import numpy as np
import socket
import time
from datetime import datetime, date
from functools import wraps, partial
from pathlib import Path
from typing import Callable, Optional, Union, Coroutine, Any
from purr_geographix.core.logger import logger


def async_wrap(func: callable) -> Callable[..., Coroutine[Any, Any, Any]]:
    """Decorator to allow running a synchronous function in a separate thread.

    Args:
        func (callable: The synchronous function

    Returns:
        callable: A new asynchronous function that runs the original
        synchronous function in an executor.
    """

    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


def is_valid_dir(fs_path: str) -> Optional[str]:
    path = Path(fs_path).resolve()
    if path.is_dir():
        return str(path)
    else:
        return None


def generate_repo_id(fs_path: str) -> str:
    """Construct a name + hash id: three chars from name + short hash

    Repos resolved via UNC path vs. drive letter will get different IDs.
    This is intentional.

    Examples:
        //scarab/ggx_projects\blank_us_nad27_mean ~~> "BLA_0F0588"

    Args:
        fs_path (str): Full path to a repo (project) directory.

    Returns:
        str: Short, unique id that is easier for humans to work with than UUID
    """
    fp = Path(fs_path)
    print(str(fp))
    prefix = fp.name.upper()[:3]
    suffix = hashlib.md5(str(fp).lower().encode()).hexdigest()[:6]
    return f"{prefix}_{suffix}".upper()


def hostname():
    return socket.gethostname().lower()


# def hashify(value: Union[str, bytes]) -> str:
#     if isinstance(value, str):
#         value = value.lower().encode("utf-8")
#     if isinstance(value, bytes):
#         value = value.decode("utf-8")
#     uuid_obj = uuid.uuid5(uuid.NAMESPACE_OID, value)
#     return str(uuid_obj)


########################


class CustomJSONEncoder(json.JSONEncoder):
    """
    A rather overwrought sanitizer for data coming from GeoGraphix projects.
    We are generally dealing with Windows CP1252 encoding, but there have been
    generations of exceptions allowed into the gxdb over the years.
    """

    def default(self, obj):
        if isinstance(obj, (str, int, bool, type(None))):
            return obj
        elif isinstance(obj, (pd.Timestamp, datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, np.floating, float)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return int(obj) if isinstance(obj, np.integer) else float(obj)
        elif isinstance(obj, np.ndarray):
            return [self.default(x) for x in obj.tolist()]
        elif isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        elif pd.api.types.is_scalar(obj):
            return None if pd.isna(obj) else obj
        elif isinstance(obj, dict):
            return {k: self.default(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.default(v) for v in obj]
        elif isinstance(obj, pd.Series):
            return self.default(obj.tolist())
        elif isinstance(obj, pd.DataFrame):
            return self.default(obj.to_dict(orient="records"))
        else:
            try:
                return super().default(obj)
            except TypeError:
                return str(obj)

    def encode(self, obj):
        def nan_to_null(o):
            if isinstance(o, float) and np.isnan(o):
                return None
            elif isinstance(o, dict):
                return {k: nan_to_null(v) for k, v in o.items()}
            elif isinstance(o, list):
                return [nan_to_null(v) for v in o]
            return o

        return json.dumps(nan_to_null(self.default(obj)), indent=4)


def datetime_formatter(format_string="%Y-%m-%dT%H:%M:%S"):
    def format_datetime(x):
        if pd.isna(x) or x == "":
            return None
        if isinstance(x, (pd.Timestamp, datetime, date)):
            return x.strftime(format_string)
        return x

    return format_datetime


def safe_numeric(x):
    # see usage in handle_query()
    if pd.isna(x) or x == "":
        return None
    try:
        result = pd.to_numeric(x, errors="coerce")
        return None if pd.isna(result) else result
    except (ValueError, TypeError, OverflowError):
        return None


def timestamp_filename(repo_id: str, asset: str, ext: str = "json"):
    """Simple file name generator for JSON exports

    Examples:
        <repo_id> _ <timestamp> _ <asset> . json
        nor_bd29a9_1721144912_well.json

    Args:
        repo_id (str): The repo_id (three-letter + hash)
        asset (str): The specific (enum) data type from which this data came.
        ext (Optional[str]): file extension--json for now

    Returns:
        str: A plausibly unique export file name
    """
    return f"{repo_id}_{int(time.time())}_{asset}.{ext}".lower()


def debugger(func: Callable[..., Any]) -> Callable[..., Any]:
    """A decorator that can log just about everything. Probably overkill.

    https://ankitbko.github.io/blog/2021/04/logging-in-python/

    Args:
        func: anything (except a generator, I think)

    Returns:
        the wrapped function
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logger.debug(f"DEBUGGER [{func.__name__}] w/args: {signature}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"DEBUGGER [{func.__name__}] result: {result}")
            return result
        except Exception as e:
            logger.exception(
                f"DEBUGGER Exception raised [{func.__name__}]. exception: {str(e)}"
            )
            raise e

    return wrapper
