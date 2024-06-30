import pyodbc
import dask.dataframe as dd
from dask.distributed import Client
from dask import delayed
from multiprocessing import freeze_support
import pandas as pd
import json
import numpy as np
from datetime import datetime, date

"""
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
"""


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


"""
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (str, int, float, bool, type(None))):
            return obj  # These types are already JSON serializable
        elif isinstance(obj, (pd.Timestamp, datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, np.floating)):
            return int(obj) if isinstance(obj, np.integer) else float(obj)
        elif isinstance(obj, np.ndarray):
            return self.default(obj.tolist())  # Recursively encode numpy arrays
        elif isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        elif isinstance(obj, (float, np.float64, np.float32)) and (
            np.isnan(obj) or np.isinf(obj)
        ):
            return None
        elif pd.api.types.is_scalar(obj) and pd.isna(obj):
            return None
        elif isinstance(obj, dict):
            return {
                k: self.default(v) for k, v in obj.items()
            }  # Recursively encode dict values
        elif isinstance(obj, list):
            return [self.default(v) for v in obj]  # Recursively encode list items
        else:
            try:
                return super().default(obj)
            except TypeError:
                return str(obj)  # Convert to string if all else fails

    def encode(self, obj):
        return json.dumps(self.default(obj), indent=4)
"""


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


def build_where_clause(index_col="uwi", condition=None, uwi_filter=None):
    clauses = [
        f"{index_col} SIMILAR TO '{uwi_filter}'" if uwi_filter else None,
        condition,
    ]
    valid_conditions = [cond for cond in clauses if cond]

    if valid_conditions:
        return f"WHERE {' AND '.join(valid_conditions)}"
    return ""


def read_sql_table_chunked(
        conn_str,
        table_name,
        index_col,
        uwi_filter=None,
        condition=None,
        chunksize=100000,
):
    where_clause = build_where_clause(index_col, condition, uwi_filter)

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
                f"{table_name}  {where_clause}  ORDER BY {index_col} "
            )

            print(query)

            with pyodbc.connect(**conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(query)

                rows = []
                for row in cursor.fetchall():
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
        return dd.from_pandas(pd.DataFrame())

    return dd.from_pandas(pd.concat(chunks), chunksize=chunksize)


# def read_sql_table_chunked(
#     conn_str,
#     table_name,
#     index_col,
#     uwi_filter=None,
#     condition=None,
#     chunksize=100000,
#     max_chunks=100,
# ):
#     where_clause = build_where_clause(condition, uwi_filter)
#
#     def get_column_mappings():
#         with pyodbc.connect(**conn_str) as conn:
#             cursor = conn.cursor()
#             return {
#                 x.column_name.lower(): x.type_name
#                 for x in cursor.columns(table=table_name)
#             }
#
#     col_mappings = get_column_mappings()
#     columns = list(col_mappings.keys())
#
#     @delayed
#     def read_chunk(start_at):
#         query = (
#             f"SELECT TOP {chunksize} START AT {start_at + 1} * FROM "
#             f"{table_name} {where_clause} ORDER BY {index_col}"
#         )
#         print(query)
#
#         with pyodbc.connect(**conn_str) as conn:
#             cursor = conn.cursor()
#             cursor.execute(query)
#
#             rows = []
#             for row in cursor.fetchall():
#                 formatted_row = []
#                 for i, cell in enumerate(row):
#                     dt = col_mappings[columns[i]]
#                     if dt == "integer":
#                         fr = formatters.get("int", lambda x: x)(cell)
#                     elif dt == "long binary":
#                         fr = formatters.get("hex", lambda x: x)(cell)
#                     elif dt in ("date", "datetime", "timestamp"):
#                         fr = formatters.get("date", lambda x: x)(cell)
#                     elif dt in ("double", "numeric"):
#                         fr = formatters.get("float", lambda x: x)(cell)
#                     else:
#                         fr = formatters.get("str", lambda x: x)(cell)
#                     formatted_row.append(fr)
#                 rows.append(formatted_row)
#
#         if not rows:
#             return None
#
#         df = pd.DataFrame(rows, columns=columns)
#         for col, dtype in col_mappings.items():
#             if dtype == "integer" and col in df.columns:
#                 df[col] = df[col].astype("Int64")
#         return df
#
#     def process_chunks(num_chunks):
#         all_dfs = []
#         for i in range(num_chunks):
#             df = read_chunk(i * chunksize).compute()
#             if df is None or df.empty:
#                 break
#             all_dfs.append(df)
#         return pd.concat(all_dfs) if all_dfs else pd.DataFrame()
#
#     # Process chunks in batches
#     batch_size = 10
#     result_dfs = []
#     for i in range(0, max_chunks, batch_size):
#         batch_df = process_chunks(min(batch_size, max_chunks - i))
#         if batch_df.empty:
#             break
#         result_dfs.append(batch_df)
#
#     if not result_dfs:
#         return dd.from_pandas(pd.DataFrame(), npartitions=1)
#
#     final_df = pd.concat(result_dfs)
#     return dd.from_pandas(final_df, npartitions=max(1, len(final_df) // chunksize))


# Usage

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

"""
def collect_and_assemble_docs(args):
    singles = []
    rollups = []

    print("STARTED TO PRIMARY DDF", str(datetime.now()))

    primary_ddf = read_sql_table_chunked(
        conn_str=conn_str,
        table_name=args["primary"]["table_name"],
        index_col=args["primary"]["index_col"],
        # condition=args["primary"]["condition"],
        uwi_filter=args.get("uwi_filter", None),
    )

    primary_df = primary_ddf.compute().set_index(
        args["primary"]["index_col"], drop=False
    )
    # print(primary_df.index.duplicated().any())

    print("STARTED TO SINGLES DDF", str(datetime.now()))

    if "singles" in args:
        for single in args["singles"]:
            single_ddf = read_sql_table_chunked(
                conn_str=conn_str,
                table_name=single["table_name"],
                index_col=single["index_col"],
                condition=single.get("condition"),
                uwi_filter=args.get("uwi_filter", None),
            )
            single_df = single_ddf.compute().set_index(single["index_col"], drop=False)
            # single_df = single_ddf.compute().set_index(["uwi", "core_id"], drop=False)
            single_df = single_df.rename(columns={"uwi": "uwi_link"})
            # print(single_df)
            # print("single==>", single_df.index.duplicated().any())
            singles.append(single_df)

    print("STARTED TO ROLLUPS DDF", str(datetime.now()))

    if "rollups" in args:
        for rollup in args["rollups"]:
            rollup_ddf = read_sql_table_chunked(
                conn_str=conn_str,
                table_name=rollup["table_name"],
                index_col=rollup["index_col"],
                condition=rollup.get("condition", None),
                uwi_filter=args.get("uwi_filter", None),
            )

            rollup_df = rollup_ddf.compute().set_index(rollup["index_col"], drop=False)
            rollup_df = rollup_df.rename(columns={"uwi": "uwi_link"})
            # print(rollup_df.index.duplicated().any())
            rollups.append(rollup_df)

    records = []

    print("STARTED TO MERGE DICTS", str(datetime.now()))

    primary_dict = primary_df.to_dict(orient="index")

    for idx, primary_row in primary_dict.items():
        o = {args["primary"]["table_name"]: primary_row}

        print(idx)

        for i, s_df in enumerate(singles):
            table_name = args["singles"][i]["table_name"]
            s_dict = s_df.to_dict(orient="index")
            # print(s_dict)
            o[table_name] = s_dict.get(idx, {col: [] for col in s_df.columns})

        for i, r_df in enumerate(rollups):
            table_name = args["rollups"][i]["table_name"]
            group_by = args["rollups"][i]["group_by"]
            r_agg = r_df.groupby(group_by).agg(lambda x: x.tolist())
            r_dict = r_agg.to_dict(orient="index")
            # o[table_name] = r_dict.get(idx, {col: [] for col in r_df.columns})
            o[table_name] = r_dict.get(idx, None)  # avoid adding blank arrays

        records.append(o)

    print("RETURNED RECORDS", str(datetime.now()))
    return records
"""


def collect_and_assemble_docs(args):
    singles = []
    rollups = []

    print("STARTED TO PRIMARY DDF", str(datetime.now()))

    primary_ddf = read_sql_table_chunked(
        conn_str=conn_str,
        table_name=args["primary"]["table_name"],
        index_col=args["primary"]["index_col"],
        # condition=args["primary"]["condition"],
        uwi_filter=args.get("uwi_filter", None),
    )

    if len(primary_ddf.index) < 1:
        print("NO RESULTS ")
        return []

    primary_df = primary_ddf.compute().set_index(
        args["primary"]["index_col"], drop=False
    )
    # print(primary_df.index.duplicated().any())

    print("STARTED TO SINGLES DDF", str(datetime.now()))

    if "singles" in args:
        for single in args["singles"]:
            single_ddf = read_sql_table_chunked(
                conn_str=conn_str,
                table_name=single["table_name"],
                index_col=single["index_col"],
                condition=single.get("condition"),
                uwi_filter=args.get("uwi_filter", None),
            )
            # add a uwi column if the field is something else (e.g. "well_id")
            if single["index_col"] != "uwi":
                single_ddf["uwi"] = single_ddf[single["index_col"]]
                single["index_col"] = "uwi"
                print(single_ddf)

            single_df = single_ddf.compute().set_index(single["index_col"], drop=False)
            # single_df = single_ddf.compute().set_index(["uwi", "core_id"], drop=False)
            single_df = single_df.rename(columns={"uwi": "uwi_link"})
            # print(single_df)
            # print("single==>", single_df.index.duplicated().any())
            singles.append(single_df)

    print("STARTED TO ROLLUPS DDF", str(datetime.now()))

    if "rollups" in args:
        for rollup in args["rollups"]:
            rollup_ddf = read_sql_table_chunked(
                conn_str=conn_str,
                table_name=rollup["table_name"],
                index_col=rollup["index_col"],
                condition=rollup.get("condition", None),
                uwi_filter=args.get("uwi_filter", None),
            )

            rollup_df = rollup_ddf.compute().set_index(rollup["index_col"], drop=False)
            rollup_df = rollup_df.rename(columns={"uwi": "uwi_link"})
            # print(rollup_df.index.duplicated().any())
            rollups.append(rollup_df)

    records = []

    print("STARTED TO MERGE DICTS", str(datetime.now()))

    primary_dict = primary_df.to_dict(orient="index")

    # Prepare singles
    print("STARTED TO singles DICTS", str(datetime.now()))
    singles_dict = {}
    for i, s_df in enumerate(singles):
        table_name = args["singles"][i]["table_name"]
        singles_dict[table_name] = s_df.to_dict(orient="index")

    # Prepare rollups
    print("STARTED TO rollups DICTS", str(datetime.now()))
    rollups_dict = {}
    for i, r_df in enumerate(rollups):
        table_name = args["rollups"][i]["table_name"]
        group_by = args["rollups"][i]["group_by"]
        r_agg = r_df.groupby(group_by).agg(lambda x: x.tolist()).to_dict(orient="index")
        rollups_dict[table_name] = r_agg

    # Create a function to assemble a single record
    print("STARTED assemble ", str(datetime.now()))

    def assemble_record(idx):
        o = {args["primary"]["table_name"]: primary_dict[idx]}

        for table_name, s_dict in singles_dict.items():
            o[table_name] = s_dict.get(idx, {col: [] for col in singles[0].columns})

        for table_name, r_dict in rollups_dict.items():
            o[table_name] = r_dict.get(idx, None)

        return o

    # Use list comprehension to create records
    records = [assemble_record(idx) for idx in primary_dict.keys()]
    print("RETURNED RECORDS", str(datetime.now()))
    return records

    # for idx, primary_row in primary_dict.items():
    #     o = {args["primary"]["table_name"]: primary_row}
    #
    #     print(idx)
    #
    #     for i, s_df in enumerate(singles):
    #         table_name = args["singles"][i]["table_name"]
    #         s_dict = s_df.to_dict(orient="index")
    #         # print(s_dict)
    #         o[table_name] = s_dict.get(idx, {col: [] for col in s_df.columns})
    #
    #     for i, r_df in enumerate(rollups):
    #         table_name = args["rollups"][i]["table_name"]
    #         group_by = args["rollups"][i]["group_by"]
    #         r_agg = r_df.groupby(group_by).agg(lambda x: x.tolist())
    #         r_dict = r_agg.to_dict(orient="index")
    #         # o[table_name] = r_dict.get(idx, {col: [] for col in r_df.columns})
    #         o[table_name] = r_dict.get(idx, None)  # avoid adding blank arrays
    #
    #     records.append(o)
    #
    # print("RETURNED RECORDS", str(datetime.now()))
    # return records


def main():
    client = Client()
    # client.cluster.scheduler.allowed_graph_size = 2e9

    ######################

    completion_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "rollups": [
            {
                "table_name": "well_completion",
                "index_col": "uwi",
                "group_by": "uwi",
            }
        ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
    }

    # Should be double-nesting based on well_core.core_id
    core_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "rollups": [
            {
                "table_name": "well_core",
                "index_col": "uwi",
                "group_by": "uwi",
            },
            {
                "table_name": "well_core_sample_anal",
                "index_col": "uwi",
                "group_by": "uwi",
            },
        ],
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "05057064630000",
    }

    dst_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "rollups": [
            {
                "table_name": "well_test",
                "index_col": "uwi",
                "group_by": "uwi",
                "condition": "test_type = 'DST'",
            },
            {
                "table_name": "well_test_pressure",
                "index_col": "uwi",
                "group_by": "uwi",
            },
            {
                "table_name": "well_test_recovery",
                "index_col": "uwi",
                "group_by": "uwi",
                "condition": "recovery_method = 'PIPE'",
            },
            {
                "table_name": "well_test_flow",
                "index_col": "uwi",
                "group_by": "uwi",
            },
        ],
        "uwi_filter": "0500105%",
    }

    formation_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "rollups": [
            {
                "table_name": "well_formation",
                "index_col": "uwi",
                "group_by": "uwi",
            }
        ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
    }

    ip_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "rollups": [
            {
                "table_name": "well_test",
                "index_col": "uwi",
                "group_by": "uwi",
                "condition": "test_type = 'IP'",
            }
        ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
    }

    perforation_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "rollups": [
            {
                "table_name": "well_perforation",
                "index_col": "uwi",
                "group_by": "uwi",
            }
        ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
    }

    # should be double-nesting based on well_cumulative_production.zone_id
    production_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "rollups": [
            {
                "table_name": "well_cumulative_production",
                "index_col": "uwi",
                "group_by": "uwi",
            },
            {
                "table_name": "gx_pden_vol_sum_by_month",
                "index_col": "uwi",
                "group_by": "uwi",
            },
        ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
        # "uwi_filter": "0500%",
    }

    raster_log_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "singles": [
            # {
            #     "table_name": "log_depth_cal_vec",
            #     "index_col": "uwi",
            # },
            {
                "table_name": "log_image_reg_log_section",
                "index_col": "well_id",
            },
        ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
    }

    survey_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "singles": [
            {
                "table_name": "well_dir_srvy",
                "index_col": "uwi",
            }
        ],
        "rollups": [
            {
                "table_name": "well_dir_srvy_station",
                "index_col": "uwi",
                "group_by": "uwi",
            }
        ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
        # "uwi_filter": "0500%",
    }

    vector_log_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "singles": [
            {
                "table_name": "gx_well_curve",
                "index_col": "wellid",
            },
            {
                "table_name": "gx_well_curveset",
                "index_col": "wellid",
            },
            {
                "table_name": "gx_well_curve_values",
                "index_col": "wellid",
            },
        ],
        # "rollups": [
        #     {
        #         "table_name": "well_dir_srvy_station",
        #         "index_col": "uwi",
        #         "group_by": "uwi",
        #     }
        # ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
    }

    well_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        "uwi_filter": "050010902%|050130657%|0504507834%",
        # "uwi_filter": "050%",
    }

    zone_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        # "singles": [
        #     {
        #         "table_name": "well_zone_interval",
        #         "index_col": "uwi",
        #     }
        # ],
        "rollups": [
            {
                "table_name": "well_zone_interval",
                "index_col": "uwi",
                "group_by": "uwi",
            },
            {
                "table_name": "well_zone_intrvl_value",
                "index_col": "uwi",
                "group_by": "uwi",
            },
        ],
        "uwi_filter": "050010902%|050130657%|0504507834%",
    }

    ######################

    # records = collect_and_assemble_docs(completion_args)
    # records = collect_and_assemble_docs(core_args)
    # records = collect_and_assemble_docs(dst_args)
    # records = collect_and_assemble_docs(formation_args)
    # records = collect_and_assemble_docs(ip_args)
    # records = collect_and_assemble_docs(perforation_args)
    # records = collect_and_assemble_docs(production_args)
    # records = collect_and_assemble_docs(raster_log_args) # broken
    # records = collect_and_assemble_docs(survey_args)
    # records = collect_and_assemble_docs(vector_log_args)  # broken
    # records = collect_and_assemble_docs(well_args)
    records = collect_and_assemble_docs(zone_args)

    myjson = json.dumps(records, indent=4, cls=CustomEncoder)
    # print(myjson)
    with open("output.txt", "w") as file:
        file.write(myjson)

    client.close()


if __name__ == "__main__":
    freeze_support()
    main()
