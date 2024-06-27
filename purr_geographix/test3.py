import pandas
import pyodbc
import dask.dataframe as dd
from dask.distributed import Client
from multiprocessing import freeze_support
import pandas as pd
import json
import numpy as np
from datetime import datetime, date


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (pd.Timestamp, datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, np.floating)):
            return int(obj) if isinstance(obj, np.integer) else float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        elif pd.isna(obj) or (isinstance(obj, float) and np.isnan(obj)):
            return None
        elif isinstance(obj, float) and np.isinf(obj):
            return None  # or you could use "Infinity" or "-Infinity" as strings
        return super().default(obj)


def replace_nan_inf(obj):
    if isinstance(obj, dict):
        return {k: replace_nan_inf(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_inf(v) for v in obj]
    elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    return obj


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


formatters = {
    "date": datetime_formatter(),
    "float": lambda x: safe_numeric(x),
    "int": lambda x: safe_numeric(x),
    "hex": lambda x: (
        x.hex() if isinstance(x, bytes) else (None if pd.isna(x) else str(x))
    ),
    "str": lambda x: str(x) if pd.notna(x) else "",
}


##############################################################################


def read_sql_table_chunked(table_name, conn_str, index_col, chunksize=100000):
    def get_column_mappings():
        with pyodbc.connect(**conn_str) as conn:
            cursor = conn.cursor()
            # return {
            #     x.column_name: (x.type_name, x.column_size)
            #     for x in cursor.columns(table=table_name)
            # }
            return {
                x.column_name: x.type_name for x in cursor.columns(table=table_name)
            }

    col_mappings = get_column_mappings()
    columns = list(col_mappings.keys())

    # 05057064630000
    def read_chunk():
        start_at = 0
        while True:
            query = (
                f"SELECT TOP {chunksize} START AT {start_at + 1} * FROM "
                f"{table_name}  WHERE uwi in ('05001050000000', '05001050010000', "
                f"'05001050030000'"
                f")  ORDER "
                f"BY {index_col} "
            )
            # query = (
            #     f"SELECT TOP {chunksize} START AT {start_at + 1} * FROM "
            #     f"{table_name}  where uwi in ('42249022870000', '42249022920000', '42249023040000')  ORDER BY {index_col} "
            # )
            with pyodbc.connect(**conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(query)

                rows = []
                for row in cursor.fetchall():
                    ###
                    formatted_row = []
                    for i, cell in enumerate(row):
                        dt = col_mappings[columns[i]]
                        if dt == "integer":
                            fr = formatters.get("int", lambda x: x)(cell)
                        elif dt == "long binary":
                            fr = formatters.get("hex", lambda x: x)(cell)
                        elif dt in ("date", "datetime", "timestamp"):
                            fr = formatters.get("date", lambda x: x)(cell)
                        elif dt in ("double", "numeric"):
                            fr = formatters.get("float", lambda x: x)(cell)
                        else:
                            fr = formatters.get("str", lambda x: x)(cell)

                        formatted_row.append(fr)
                    rows.append(formatted_row)

                df = pd.DataFrame(rows, columns=columns)

                for col, dtype in col_mappings.items():
                    if dtype == "integer" and col in df.columns:
                        df[col] = df[col].astype("Int64")

                print(df)

            if df.empty:
                break
            yield df
            start_at += chunksize

    chunks = list(read_chunk())

    if not chunks:
        return pd.DataFrame()

    return dd.from_pandas(pd.concat(chunks), chunksize=chunksize)


##############################################################################
####################################
##############


def rollup_child_rows(parent_df, child_df, composite_key):
    # Identify the columns that are unique to the child table
    child_unique_columns = child_df.columns.difference(parent_df.columns)

    # Create a dictionary of aggregation functions
    agg_dict = {col: list for col in child_unique_columns}

    # Group by the composite key and aggregate the child-specific columns into lists
    rolled_up_child = child_df.groupby(composite_key).agg(agg_dict)

    # Merge the rolled-up child data with the parent data
    result = parent_df.merge(rolled_up_child, on=composite_key, how="left")

    return result


# def create_nested_dict(
#     parent_df, child_df, composite_key, parent_table_name, child_table_name
# ):
#     parent_dict = parent_df.set_index(composite_key).to_dict(orient="index")
#     child_dict = (
#         child_df.groupby(composite_key)
#         .apply(lambda x: x.to_dict(orient="records"))
#         .to_dict()
#     )
#
#     nested_dict = {}
#     for key in parent_dict.keys():
#         nested_dict[key] = {
#             parent_table_name: parent_dict[key],
#             child_table_name: child_dict.get(key, []),
#         }
#
#     return nested_dict


# def create_nested_dict(
#     parent_df, child_df, composite_key, parent_table_name, child_table_name
# ):
#     parent_dict = parent_df.set_index(composite_key).to_dict(orient="index")
#
#     # Aggregate child DataFrame values into lists for each key
#     child_agg = child_df.groupby(composite_key).agg(list).reset_index()
#     child_dict = child_agg.set_index(composite_key).to_dict(orient="index")
#
#     nested_dict = {}
#     for key in parent_dict.keys():
#         nested_dict[key] = {
#             parent_table_name: parent_dict[key],
#             child_table_name: child_dict.get(
#                 key, {col: [] for col in child_df.columns if col not in composite_key}
#             ),
#         }
#
#     return nested_dict


def create_nested_dict(
        parent_df, child_df, composite_key, parent_table_name, child_table_name
):
    parent_dict = parent_df.to_dict(orient="index")

    # Aggregate child DataFrame values into lists for each key
    child_agg = child_df.groupby(composite_key).agg(lambda x: x.tolist()).reset_index()
    child_dict = child_agg.to_dict(orient="index")

    # cleaned_dict = replace_nan_inf(child_dict)
    # myjson = json.dumps(cleaned_dict, indent=4, cls=CustomEncoder)
    # exit(0)
    # print(myjson)

    # nested_dict = {}
    stuff = []

    for idx, parent_row in parent_dict.items():
        # key = str(tuple(parent_row[k] for k in composite_key))
        # nested_dict[key] = {
        #     parent_table_name: parent_row,
        #     child_table_name: child_dict.get(
        #         idx, {col: [] for col in child_df.columns}
        #     ),
        # }
        stuff.append(
            {
                parent_table_name: parent_row,
                child_table_name: child_dict.get(
                    idx, {col: [] for col in child_df.columns}
                ),
            }
        )

    # print(stuff)
    myjson = json.dumps(stuff, indent=4, cls=CustomEncoder)
    print(myjson)
    return stuff
    # return nested_dict


# def create_nested_dict(
#     parent_df, child_df, composite_key, parent_table_name, child_table_name
# ):
#     parent_dict = parent_df.to_dict(orient="records")
#
#     # Aggregate child DataFrame values into lists for each key
#     child_agg = child_df.groupby(composite_key).agg(lambda x: x.tolist()).reset_index()
#     child_dict = child_agg.to_dict(orient="records")
#
#     nested_dict = {}
#     for parent_row in parent_dict:
#         key = str(tuple(parent_row[k] for k in composite_key))
#         child_row = next(
#             (
#                 item
#                 for item in child_dict
#                 if all(item[k] == parent_row[k] for k in composite_key)
#             ),
#             {},
#         )
#
#         # Remove composite key from child_row
#         for k in composite_key:
#             child_row.pop(k, None)
#
#         nested_dict[key] = {parent_table_name: parent_row, child_table_name: child_row}
#
#     cleaned_dict = replace_nan_inf(nested_dict)
#     myjson = json.dumps(cleaned_dict, indent=4, cls=CustomEncoder)
#     print(myjson)
#     # return nested_dict


##############
####################################
##############################################################################


conn_str = {
    "astart": "YES",
    "dbf": "\\\\scarab\\ggx_projects\\Colorado_North\\gxdb.db",
    "dbn": "Colorado_North-ggx_projects",
    "driver": "SQL Anywhere 17",
    "host": "scarab",
    "pwd": "sql",
    "server": "GGX_SCARAB",
    "uid": "dba",
    "port": None,
    "CharSet": "cp1252",
}


def main():
    client = Client()

    parent_ddf = read_sql_table_chunked("well", conn_str, index_col="UWI")
    child_ddf = read_sql_table_chunked("well_formation", conn_str, index_col="UWI")

    composite_key = ["UWI"]
    parent_table_name = "well"
    child_table_name = "well_formation"

    parent_df = parent_ddf.compute()
    child_df = child_ddf.compute()

    nested_dict = create_nested_dict(
        parent_df, child_df, composite_key, parent_table_name, child_table_name
    )

    # nested_dict = create_nested_dict(
    #     parent_df, child_df, composite_key, parent_table_name, child_table_name
    # )
    #
    # cleaned_dict = replace_nan_inf(nested_dict)
    # json_cols = json.dumps(cleaned_dict, indent=4, cls=CustomEncoder)

    # print(json_cols)

    client.close()


# def main():
#     client = Client()
#
#     parent_df = read_sql_table_chunked("well", conn_str, index_col="UWI")
#     child_df = read_sql_table_chunked("well_formation", conn_str, index_col="UWI")
#
#     composite_key = ["UWI"]
#     merged_df = rollup_child_rows(parent_df, child_df, composite_key)
#
#     result = merged_df.compute()
#     result.columns = result.columns.str.lower()
#
#     dict_cols = result.to_dict(orient="records")
#     cleaned_dict = replace_nan_inf(dict_cols)
#     json_cols = json.dumps(cleaned_dict, indent=4, cls=CustomEncoder)
#
#     print(json_cols)
#
#     client.close()


if __name__ == "__main__":
    freeze_support()
    main()
