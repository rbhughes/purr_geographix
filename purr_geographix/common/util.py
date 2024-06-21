from purr_geographix.api_modules.database import get_db
import hashlib
import asyncio
import uuid
from functools import wraps, partial

# import os
from pathlib import Path
from typing import Optional, Union

# from flounder.models.models_a import User


def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run


def say(stuff: str):
    db = next(get_db())
    # res = db.query(User).all()

    # res = db.query(User).first()
    # res = db.query(User).all()
    # res = db.query(User).filter(User.name == stuff).all()

    # users = crud_a.get_users(db, skip=skip, limit=limit)
    print("SSSSSS should be user SSSSSSSSSSSSSSSSSSS")
    print(type(stuff))
    # print(res)
    print("SSSSSSSSSSSSSSSSSSSSSSSSS")


# def normalize_path(fs_path: str) -> str:
#     """
#     Assuming the string is a valid path, replace all backslashes with forward
#     slashes. This avoids the double-escape madness on UNC paths. Windows can
#     usually resolve forward slash UNCs like: //server/share/path
#     :param fs_path: Any path string
#     :return: path with forward slashes
#     """
#     return fs_path.replace("\\", "/")
#
#
# def dir_exists(fs_path: str) -> bool:
#     """
#     Is the supplied path a valid directory from the context of this PC and user?
#     :param fs_path: A file path
#     :return: True if valid
#     """
#     return os.path.isdir(fs_path)


def validate_dir_path(fs_path: str) -> Optional[str]:
    try:
        normed = Path(fs_path).resolve()
        if normed.is_dir():
            return str(normed)
        return None
    except Exception:
        return None


def hashify(value: Union[str, bytes]) -> str:
    """
    Calculate the MD5 hash of the given value.

    Args:
        value (Union[str, bytes]): The input value to be hashed. If a string
            is provided, it will be converted to bytes using UTF-8 encoding.

    Returns:
        str: The UUID5 hash of the input value as a string.
    """
    if isinstance(value, str):
        value = value.lower().encode("utf-8")

    uuid_obj = uuid.uuid5(uuid.NAMESPACE_OID, value.decode('utf-8') if isinstance(value, bytes) else value)
    return str(uuid_obj)


def make_repo_uuid(repo_fs: str):
    hash_object = hashlib.md5(repo_fs.encode())
    hex_hash = hash_object.hexdigest()
    return uuid.UUID(hex_hash)
