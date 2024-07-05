import asyncio
import hashlib
import uuid
import json
import pandas as pd
import numpy as np
from datetime import datetime, date
from functools import wraps, partial
from pathlib import Path
from typing import Optional, Union


def async_wrap(func):
    """
    Decorator to allow running a synchronous function in a separate thread.

    Args:
        func (callable): The synchronous function to be decorated.

    Returns:
        callable: A new asynchronous function that runs the original synchronous
        function in an executor.
    """

    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


def is_valid_dir(fs_path: str) -> Optional[str]:
    """
    Check if the given file system path is a valid directory.
    Args:
        fs_path (str): The file system path to check.

    Returns:
        Optional[str]: Resolved path if it is a valid directory, otherwise None.
    """
    path = Path(fs_path).resolve()
    if path.is_dir():
        return str(path)
    else:
        return None


def generate_repo_id(fs_path: str):
    """
    Construct a name + hash id using the pathlib Path. First three chars are
    from the name, last six from a hash of the path:
    //scarab/ggx_projects\blank_us_nad27_mean ~~> "BLA_0F0588"
    Path strings are lowercased to standardize the hash.

    NOTE: Repos resolved via UNC path vs. drive letter will get different IDs.
    This is intentional.

    :param fs_path: full path to a repo
    :return: unique id string
    """
    fp = Path(fs_path)
    print(str(fp))
    prefix = fp.name.upper()[:3]
    suffix = hashlib.md5(str(fp).lower().encode()).hexdigest()[:6]
    return f"{prefix}_{suffix}".upper()


def hashify(value: Union[str, bytes]) -> str:
    """
    Calculate the UUID-like hash string for a given string/byte sequence.

    Args:
        value (Union[str, bytes]): The input value to be hashed. If a string
        is provided, it will be converted to bytes using UTF-8 encoding.

    Returns:
        str: The UUID5 hash of the input value as a string.
    """
    if isinstance(value, str):
        value = value.lower().encode("utf-8")

    uuid_obj = uuid.uuid5(
        uuid.NAMESPACE_OID, value.decode("utf-8") if isinstance(value, bytes) else value
    )
    return str(uuid_obj)


########################


class CustomEncoder(json.JSONEncoder):
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
        def nan_to_null(obj):
            if isinstance(obj, float) and np.isnan(obj):
                return None
            elif isinstance(obj, dict):
                return {k: nan_to_null(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [nan_to_null(v) for v in obj]
            return obj

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
    if pd.isna(x) or x == "":
        return None
    try:
        return pd.to_numeric(x, errors="coerce")
    except:
        return None
