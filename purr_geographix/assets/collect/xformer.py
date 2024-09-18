"""GeoGraphix xformers"""

import re
import struct
import numpy as np

import pandas as pd
from datetime import datetime, timedelta

from typing import Any, Dict, Optional, List, Generator, Union

PURR_NULL = "_purrNULL_"
PURR_DELIM = "_purrDELIM_"
PURR_WHERE = "_purrWHERE_"

################################################################################


def map_col_type(sql_type):
    """
    Map SQL data types to pandas data types.
    """
    cursor_types = {
        "str": "string",
        "int": "int64",
        "float": "float64",
        "bool": "bool",
        "datetime": "datetime64[ns]",
        "Decimal": "float64",
        "bytearray": "object",  # TODO: check this binary/blob
        type(None): "object",
    }

    return cursor_types.get(sql_type, "object")


def get_column_info(cursor):
    """todo"""
    cursor_desc = cursor.description
    column_names = [col[0] for col in cursor_desc]
    column_types = {col[0]: map_col_type(col[1].__name__) for col in cursor_desc}
    return column_names, column_types


def standardize_df_columns(df: pd.DataFrame, column_types: Dict[str, str]):
    """todo"""

    for col, col_type in column_types.items():
        if "int" in col_type:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
            df[col] = df[col].astype("Int64")
        elif "str" in col_type:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
            df[col] = df[col].astype("string")
        elif "datetime64[ns]" in col_type:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S").dt.floor("s")
        else:
            df[col] = df[col].astype(col_type)

    return df


################################################################################


def decode_string(buffer, start, end):
    """Decode a null-terminated string"""
    return buffer[start:end].decode().split("\x00")[0]


def unpack_double(buffer, start):
    """Unpack a double (8 bytes)"""
    return struct.unpack("<d", buffer[start : start + 8])[0]


def unpack_short(buffer, start):
    """Unpack a short(2 bytes)"""
    return struct.unpack("<h", buffer[start : start + 2])[0]


def unpack_int(buffer, start):
    """Unpack an integer (4 bytes)"""
    return struct.unpack("<i", buffer[start : start + 4])[0]


################################################################################


def safe_string(x: Optional[str]) -> Optional[str]:
    """remove control, non-printable chars, ensure UTF-8, strip whitespace."""
    if x is None:
        return None
    if str(x) == "<NA>":  # probably pandas._libs.missing.NAType'
        return None
    cleaned = re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(x))
    try:
        utf8_string = cleaned.encode("latin1").decode("utf-8")
    except UnicodeEncodeError:
        # If the string is already in UTF-8, use the cleaned version
        utf8_string = cleaned
    return "".join(char for char in utf8_string if char.isprintable()).strip()


# def safe_numeric(x, col_type: Optional[str]):
#     """try to make a pandas-friendly numeric from int, float, etc."""
#     if pd.isna(x) or x == "":
#         return None
#     try:
#         result = pd.to_numeric(x, errors="coerce")
#         return None if pd.isna(result) else result
#     except (ValueError, TypeError, OverflowError):
#         return None


def safe_bool(x: Any) -> bool:
    """make a bool"""
    if x is None:
        return False
    if isinstance(x, str):
        return x.lower().strip() == "true"
    if isinstance(x, (list, tuple, dict, set)):
        return bool(x)
    try:
        return bool(x)
    except ValueError:
        return False


def safe_float(x: Optional[float]) -> Optional[float]:
    """Convert input to a float or None"""
    if x is None or pd.isna(x):
        return None
    try:
        result = float(x)
        return None if pd.isna(result) else result
    except (ValueError, TypeError, OverflowError):
        return None


def safe_int(x: Optional[int]) -> Optional[int]:
    """Convert input to an int or None"""
    if x is None:
        return None
    try:
        result = int(x)
        return result
    except (ValueError, TypeError, OverflowError):
        return None


def safe_datetime(x: Optional[str]) -> Optional[datetime]:
    """Convert input string to a datetime object in ISO format or None"""
    if pd.isna(x):
        return None
    try:
        result = pd.to_datetime(x)
        if pd.isna(result):
            return None
        dt = result.to_pydatetime().replace(tzinfo=None, microsecond=0)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OverflowError):
        return None


def memo_to_string(x):
    """Strip control chars from DBISAM memo"""
    if x is None:
        return None
    if str(x) == "<NA>":  # probably pandas._libs.missing.NAType'
        return None
    return re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(x))


def blob_to_hex(x):
    """Just return a hex string (for json serialization)"""
    if x is None:
        return None
    return f"0x{x.hex()}"


# def decode_curve_values(x: Optional[bytes]) -> List[float]:
#     print("...............................................")
#     curve_vals = []
#     buf = bytearray(x)
#     for i in range(2, len(buf), 4):
#         cval_bytes = buf[i : i + 4]  # 32 bit float
#         cval = struct.unpack("<f", cval_bytes)[0]
#         curve_vals.append(cval)
#     return curve_vals


def decode_curve_values(x: Optional[bytes]) -> List[float]:
    if not x:
        return []

    # Slice the buffer to exclude the first two bytes
    buf = memoryview(x)[2:]

    # Use struct.unpack_from to unpack multiple floats at once
    return list(struct.unpack_from(f"<{len(buf)//4}f", buf))


def array_of_int(x):
    if pd.isna(x):
        return []
    return [safe_int(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_bool(x):
    if pd.isna(x):
        return []
    return [safe_bool(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_float(x):
    if pd.isna(x):
        return []
    return [safe_float(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_string(x):
    if pd.isna(x):
        return []
    return [safe_string(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_datetime(x):
    if pd.isna(x):
        return []
    return [safe_datetime(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


# def array_of_hex(x):
#     return [blob_to_hex(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


# def tester(x, col_type):
#     return "TESTER TESTER " + col_type


###############################################################################

# TODO: add bool and datetime64[ns]
formatters = {
    "Int64": safe_int,
    "bool": safe_bool,
    "float64": safe_float,
    "string": safe_string,
    "datetime": safe_datetime,
    "datetime64": safe_datetime,
    "memo_to_string": memo_to_string,
    "blob_to_hex": blob_to_hex,
    "array_of_int": array_of_int,
    "array_of_bool": array_of_bool,
    "array_of_float": array_of_float,
    "array_of_string": array_of_string,
    "array_of_datetime": array_of_datetime,
    "decode_curve_values": decode_curve_values,
}
