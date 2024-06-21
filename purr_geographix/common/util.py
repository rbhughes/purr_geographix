import asyncio
import uuid
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
