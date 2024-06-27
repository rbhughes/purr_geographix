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

            # NOTE: columns set to lower()
            return {
                x.column_name.lower(): x.type_name
                for x in cursor.columns(table=table_name)
            }

    col_mappings = get_column_mappings()
    columns = list(col_mappings.keys())

    # 05057064630000
    def read_chunk():
        start_at = 0
        while True:
            query = (
                f"SELECT TOP {chunksize} START AT {start_at + 1} * FROM "
                f"{table_name}  WHERE uwi in ('05001066590000', '05001069230000', "
                f"'05001090800000'"
                f")  ORDER "
                f"BY {index_col} "
            )
            # query = (
            #     f"SELECT TOP {chunksize} START AT {start_at + 1} * FROM "
            #     f"{table_name}  WHERE uwi in ('05001050000000', '05001050010000', "
            #     f"'05001050030000'"
            #     f")  ORDER "
            #     f"BY {index_col} "
            # )
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

                # print(df.columns.str.lower())

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


def maker(primary, singles, rollups):
    records = []

    prime_df, prime_name, prime_idx = primary
    prime_dict = prime_df.to_dict(orient="index")

    for idx, prime_row in prime_dict.items():
        o = {prime_name: prime_row}

        for s in singles:
            s_df, s_name, s_idx = s
            s_dict = s_df.to_dict(orient="index")
            o[s_name] = s_dict.get(idx, {col: [] for col in s_df.columns})

        for r in rollups:
            r_df, r_name, r_idx = r
            r_agg = r_df.groupby(r_idx).agg(lambda x: x.tolist()).reset_index()
            r_dict = r_agg.to_dict(orient="index")
            o[r_name] = r_dict.get(idx, {col: [] for col in r_df.columns})

        records.append(o)

    return records


# def create_nested_dict(
#     well_df, parent_df, child_df, composite_key, parent_table_name, child_table_name
# ):
#     well_dict = well_df.to_dict(orient="index")
#     parent_dict = parent_df.to_dict(orient="index")
#
#     child_agg = child_df.groupby(composite_key).agg(lambda x: x.tolist()).reset_index()
#     child_dict = child_agg.to_dict(orient="index")
#
#     records = []
#
#     for idx, well_row in well_dict.items():
#         records.append(
#             {
#                 "well": well_row,
#                 parent_table_name: parent_dict.get(
#                     idx, {col: [] for col in parent_df.columns}
#                 ),
#                 child_table_name: child_dict.get(
#                     idx, {col: [] for col in child_df.columns}
#                 ),
#             }
#         )
#
#     # for idx, parent_row in parent_dict.items():
#     #     records.append(
#     #         {
#     #             parent_table_name: parent_row,
#     #             child_table_name: child_dict.get(
#     #                 idx, {col: [] for col in child_df.columns}
#     #             ),
#     #         }
#     #     )
#
#     return records


"""
def create_parent_child_json(parent_df, child_df_list, parent_key, child_keys):
    records = []

    for _, parent_row in parent_df.iterrows():
        parent_record = parent_row.to_dict()
        parent_record["children"] = []

        for child_df in child_df_list:
            child_records = child_df[
                child_df[parent_key] == parent_row[parent_key]
            ].to_dict("records")
            for child_record in child_records:
                filtered_child_record = {
                    key: child_record[key] for key in child_keys if key in child_record
                }
                parent_record["children"].append(filtered_child_record)

        records.append(parent_record)

    return records
"""

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

    well_ddf = read_sql_table_chunked("well", conn_str, index_col="uwi")

    parent_ddf = read_sql_table_chunked("well_dir_srvy", conn_str, index_col="uwi")
    child_ddf = read_sql_table_chunked(
        "well_dir_srvy_station", conn_str, index_col="uwi"
    )

    composite_key = ["uwi"]
    parent_table_name = "well_dir_srvy"
    child_table_name = "well_dir_srvy_station"

    well_df = well_ddf.compute()
    parent_df = parent_ddf.compute()
    child_df = child_ddf.compute()

    primary = (well_df, "well", ("uwi"))
    singles = [(parent_df, "well_dir_srvy", ("uwi"))]
    rollups = [(child_df, "well_dir_srvy_station", ("uwi"))]
    records = maker(primary, singles, rollups)
    myjson = json.dumps(records, indent=4, cls=CustomEncoder)
    with open("hello.txt", mode="w") as file:
        file.write(myjson)

    # records = create_nested_dict(
    #     well_df, parent_df, child_df, composite_key, parent_table_name, child_table_name
    # )
    #
    # myjson = json.dumps(records, indent=4, cls=CustomEncoder)
    # print(myjson)
    # #
    # with open("hello.txt", mode="w") as file:
    #     file.write(myjson)

    client.close()


# def mainZ():
#     client = Client()
#
#     parent_ddf = read_sql_table_chunked("well", conn_str, index_col="uwi")
#     child_ddf = read_sql_table_chunked("well_formation", conn_str, index_col="uwi")
#
#     composite_key = ["uwi"]
#     parent_table_name = "well"
#     child_table_name = "well_formation"
#
#     parent_df = parent_ddf.compute()
#     child_df = child_ddf.compute()
#
#     records = create_nested_dict(
#         parent_df, child_df, composite_key, parent_table_name, child_table_name
#     )
#
#     myjson = json.dumps(records, indent=4, cls=CustomEncoder)
#     print(myjson)
#     #
#     # with open("hello.txt", mode="w") as file:
#     #     file.write(myjson)
#
#     client.close()


if __name__ == "__main__":
    freeze_support()
    main()
