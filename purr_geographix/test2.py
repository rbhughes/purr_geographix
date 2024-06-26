import pandas
import pyodbc
import dask.dataframe as dd
from dask.distributed import Client
from multiprocessing import freeze_support
import pandas as pd
import json
import numpy as np
from datetime import datetime, date

# conn_str = {
#     "astart": "YES",
#     "dbf": "\\\\scarab\\ggx_projects\\Colorado_North\\gxdb.db",
#     "dbn": "Colorado_North-ggx_projects",
#     "driver": "SQL Anywhere 17",
#     "host": "scarab",
#     "pwd": "sql",
#     "server": "GGX_SCARAB",
#     "uid": "dba",
#     "port": None,
#     "CharSet": "cp1252",
# }
conn_str = {
    "astart": "YES",
    "dbf": "\\\\scarab\\ggx_projects\\Stratton83\\gxdb.db",
    "dbn": "Stratton83-ggx_projects",
    "driver": "SQL Anywhere 17",
    "host": "scarab",
    "pwd": "sql",
    "server": "GGX_SCARAB",
    "uid": "dba",
    "port": None,
    # "CharSet": "cp1252",
}


# parent_query = "SELECT * from well"
# child_query = "SELECT * from well_formation"


# def datetime_formatter(format_string="%Y-%m-%dT%H:%M:%S"):
#     return lambda x: x.strftime(format_string) if isinstance(x, (datetime, date)) else x


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


def read_sql_table_chunked(table_name, conn_str, index_col, chunksize=100000):
    formatters = {
        "date": datetime_formatter(),
        "float": lambda x: safe_numeric(x),
        "int": lambda x: safe_numeric(x),
        "hex": lambda x: (
            x.hex() if isinstance(x, bytes) else (None if pd.isna(x) else str(x))
        ),
        "str": lambda x: str(x) if pd.notna(x) else "",
    }

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

    def read_chunk():
        start_at = 0
        while True:
            query = (
                f"SELECT TOP {chunksize} START AT {start_at + 1} * FROM "
                f"{table_name}  WHERE uwi LIKE '4235532996%'  ORDER BY {index_col} "
            )
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

            if df.empty:
                break
            yield df
            start_at += chunksize

    chunks = list(read_chunk())

    if not chunks:
        return pd.DataFrame()

    return dd.from_pandas(pd.concat(chunks), chunksize=chunksize)


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


def main():
    client = Client()
    # print(pd.__version__)

    try:
        parent_df = read_sql_table_chunked("well", conn_str, index_col="UWI")
        child_df = read_sql_table_chunked("well_formation", conn_str, index_col="UWI")

        # composite_key = ["UWI", "SOURCE", "FORM_ID", "FORM_OBS_NO"]
        composite_key = ["UWI"]

        merged_df = rollup_child_rows(parent_df, child_df, composite_key)

        result: pandas.DataFrame = merged_df.compute()
        # print(result[["GX_FORM_TOP_DEPTH"]].dtypes)
        result.columns = result.columns.str.lower()

        # pd.set_option("display.max_rows", None)
        # print(result.head())
        pd.set_option("display.width", None)
        pd.set_option("display.max_columns", None)

        ###
        dict_cols = result.to_dict(orient="records")
        cleaned_dict = replace_nan_inf(dict_cols)
        # json_cols = json.dumps(dict_cols, indent=4, cls=CustomEncoder)
        json_cols = json.dumps(cleaned_dict, indent=4, cls=CustomEncoder)

        print(json_cols)
        ###

        #####################

        # # cols = result[["UWI", "GX_FORM_TOP_DEPTH"]]
        # cols = result[["uwi", "gx_form_top_depth"]]
        # # cols = cols.where(pd.notnull(cols), None)
        #
        # dict_cols = cols.to_dict(orient="records")
        #
        # json_cols = json.dumps(dict_cols, indent=4, cls=CustomEncoder)
        #
        # print(json_cols)
        # print("GOT HERE")

        # print(result.head())
        # print(type(result))
        # print(result.describe())

    finally:
        # Close the Dask client
        client.close()


if __name__ == "__main__":
    freeze_support()
    main()
