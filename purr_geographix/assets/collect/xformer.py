"""GeoGraphix xformers"""

import re
import struct
import numpy as np
import pyodbc
from typing import Any, Dict, Optional, Literal, List, Union, Tuple, TypeAlias
import pandas as pd


PURR_NULL = "_purrNULL_"
PURR_DELIM = "_purrDELIM_"
PURR_WHERE = "_purrWHERE_"

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

ColTypes: TypeAlias = Union[
    Literal["string", "int64", "float64", "bool", "datetime64[ns]", "object"], str
]


def map_col_type(sql_type) -> ColTypes:
    """Map SQL data types from pyodbc/SQLAnywhere to pandas data types."""
    cursor_types = {
        "str": "string",
        "int": "int64",
        "float": "float64",
        "bool": "bool",
        "datetime": "datetime64[ns]",
        "Decimal": "float64",
        "bytearray": "object",  # any usage?
        type(None): "object",
    }

    return cursor_types.get(sql_type, "object")


def get_column_info(cursor: pyodbc.Cursor) -> Tuple[List[str], Dict[str, str]]:
    """Return column names, types from pyodbc/SQLAnywhere"""
    cursor_desc = cursor.description
    column_names = [col[0] for col in cursor_desc]
    column_types = {col[0]: map_col_type(col[1].__name__) for col in cursor_desc}
    return column_names, column_types


def standardize_df_columns(df: pd.DataFrame, column_types: Dict[str, str]):
    """Try to normalize some column types in a dataframe based on column_types
    as returned by pyodbc/SQLAnywhere."""

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


def safe_string(x: Optional[str]) -> Optional[str]:
    """Remove control, non-printable chars, ensure UTF-8, strip whitespace."""
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


def safe_bool(x: Optional[Any]) -> Optional[bool]:
    """Convert input to a bool or None"""
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


def safe_float(x: Optional[Any]) -> Optional[float]:
    """Convert input to a float or None"""
    if x is None or pd.isna(x):
        return None
    try:
        result = float(x)
        return None if pd.isna(result) else result
    except (ValueError, TypeError, OverflowError):
        return None


def safe_int(x: Optional[Any]) -> Optional[int]:
    """Convert input to an int or None"""
    if x is None:
        return None
    try:
        result = int(x)
        return result
    except (ValueError, TypeError, OverflowError):
        return None


def safe_datetime(x: Optional[str]) -> Optional[str]:
    """Convert input string to a datetime object as ISO string or None"""
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


def blob_to_hex(x):
    """Just return a hex string (for json serialization)"""
    if x is None:
        return None
    return f"0x{x.hex()}"


# less magical version...
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
    """unpack gx_well_curve_values.curve_values as List of floats"""
    if not x:
        return []
    buf = memoryview(x)[2:]
    return list(struct.unpack_from(f"<{len(buf)//4}f", buf))


################################################################################


def array_of_int(x: Optional[str]) -> List[Optional[int]]:
    """return list of int or an empty List"""
    if x is None or pd.isna(x):
        return []
    return [safe_int(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_bool(x: Optional[str]) -> List[Optional[bool]]:
    """return List of bool (or empty)"""
    if x is None or pd.isna(x):
        return []
    return [safe_bool(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_float(x: Optional[str]) -> List[Optional[float]]:
    """return List of float (or empty)"""
    if x is None or pd.isna(x):
        return []
    return [safe_float(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_string(x: Optional[str]) -> List[Optional[str]]:
    """return List of str (or empty)"""
    if x is None or pd.isna(x):
        return []
    return [safe_string(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_datetime(x: Optional[str]) -> List[Optional[str]]:
    """return List of iso date strings (or empty)"""
    if x is None or pd.isna(x):
        return []
    return [safe_datetime(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


###############################################################################


def series_row_to_json(
    row: pd.Series, prefix_mapping: Dict[str, str]
) -> Dict[str, Any]:
    """Convert a pandas Series row to a JSON-like dictionary structure."""
    result: Dict[str, Dict[str, Union[None, int, float, str, List[Any]]]] = {}
    for column, value in row.items():
        if isinstance(value, np.ndarray):
            value = value.tolist()

        # Handle list of numpy arrays
        elif isinstance(value, list):
            value = [
                item.tolist() if isinstance(item, np.ndarray) else item
                for item in value
            ]

        elif not isinstance(value, list):
            if pd.isna(value):
                value = None

        for prefix, table_name in prefix_mapping.items():
            if column.startswith(prefix):
                if table_name not in result:
                    result[table_name] = {}
                result[table_name][column[len(prefix) :]] = value
                break
    return result


def transform_dataframe_to_json(
    df: pd.DataFrame, prefix_mapping: Dict[str, str]
) -> List[Dict[str, Dict[str, Union[None, int, float, str, List[Any]]]]]:
    """Convert a DataFrame to a list of JSON-like dictionary structures."""
    return [series_row_to_json(row, prefix_mapping) for _, row in df.iterrows()]


###############################################################################

# TODO: add bool and datetime64[ns]
formatters = {
    "Int64": safe_int,
    "bool": safe_bool,
    "float64": safe_float,
    "string": safe_string,
    "datetime": safe_datetime,
    "datetime64": safe_datetime,
    "blob_to_hex": blob_to_hex,
    "array_of_int": array_of_int,
    "array_of_bool": array_of_bool,
    "array_of_float": array_of_float,
    "array_of_string": array_of_string,
    "array_of_datetime": array_of_datetime,
    "decode_curve_values": decode_curve_values,
}
