import pyodbc
from multiprocessing import freeze_support
import pandas as pd
import json
import numpy as np
from datetime import datetime, date


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


def build_where_clause(
        index_col="uwi",
        subquery=None,
        subconditions=None,
        conditions=None,
        uwi_filter=None,
):
    clauses = []

    if subquery:
        clauses.append(
            build_subquery_clause(index_col, subquery, subconditions, uwi_filter)
        )

    if conditions:
        clauses.extend(conditions)

    if uwi_filter:
        clauses.append(f"{index_col} SIMILAR TO '{uwi_filter}'")

    return f"WHERE {' AND '.join(clauses)}" if clauses else ""


def build_subquery_clause(index_col, subquery, subconditions, uwi_filter):
    subz = []

    if subconditions:
        for sc in subconditions:
            if "__uwi_sub__" in sc and uwi_filter:
                subz.append(f"{index_col} SIMILAR TO '{uwi_filter}'")
            elif "__uwi_sub__" not in sc:
                subz.append(sc)

    sub_where = f" WHERE {' AND '.join(subz)}" if subz else ""
    return f"{index_col} IN ({subquery}{sub_where})"


"""
def build_where_clause(
    index_col="uwi",
    subquery=None,
    subconditions=[],
    conditions=[],
    uwi_filter=None,
):
    clauses = []

    if subquery:
        subz = []
        sq = f"{index_col} IN ({subquery} _sub_)"
        for sc in subconditions:
            if "__uwi_sub__" in sc:
                sc = f"{index_col} SIMILAR TO '{uwi_filter}'" if uwi_filter else None
                if sc:
                    subz.append(sc)
            else:
                subz.append(sc)
        if subz:
            sq = sq.replace("_sub_", f"WHERE {' AND '.join(subz)}")
        else:
            sq = sq.replace("_sub_", "")
        clauses.append(sq)

    for cond in conditions:
        clauses.append(cond)

    if uwi_filter:
        clauses.append(f"{index_col} SIMILAR TO '{uwi_filter}'")

    if clauses:
        return f"WHERE {' AND '.join(clauses)}"
    return ""
"""


def read_sql_table_chunked(
        conn_str,
        table_name,
        index_col,
        uwi_filter=None,
        subquery=None,
        subconditions=[],
        conditions=[],
        chunksize=10000,
):
    where_clause = build_where_clause(
        index_col, subquery, subconditions, conditions, uwi_filter
    )

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

    return read_chunk()

    # chunks = list(read_chunk())
    #
    # if not chunks:
    #     return dd.from_pandas(pd.DataFrame())
    #
    # return dd.from_pandas(pd.concat(chunks), chunksize=chunksize)


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


def collect_and_assemble_docs(args):
    primary_chunks = read_sql_table_chunked(
        conn_str=conn_str,
        table_name=args["primary"]["table_name"],
        index_col=args["primary"]["index_col"],
        subquery=args["primary"].get("subquery", None),
        subconditions=args["primary"].get("subconditions", []),
        conditions=args["primary"].get("conditions", []),
        uwi_filter=args.get("uwi_filter", None),
    )

    records = []
    for primary_chunk in primary_chunks:
        if primary_chunk.empty:
            continue

        primary_chunk.set_index(args["primary"]["index_col"], drop=False, inplace=True)

        singles_dict = {}
        if "singles" in args:
            for single in args["singles"]:
                orig_index_col = single["index_col"]
                single_chunks = read_sql_table_chunked(
                    conn_str=conn_str,
                    table_name=single["table_name"],
                    index_col=single["index_col"],
                    subquery=single.get("subquery", None),
                    subconditions=single.get("subconditions", []),
                    conditions=single.get("conditions", []),
                    uwi_filter=args.get("uwi_filter", None),
                )
                single_chunk = next(single_chunks, pd.DataFrame())
                if not single_chunk.empty:
                    if single["index_col"] != "uwi":
                        single_chunk["uwi"] = single_chunk[single["index_col"]]
                        single["index_col"] = "uwi"
                    single_chunk.set_index(
                        single["index_col"], drop=False, inplace=True
                    )
                    single_chunk = single_chunk.rename(columns={"uwi": "uwi_link"})
                    singles_dict[single["table_name"]] = single_chunk.to_dict(
                        orient="index"
                    )
                # essential for when index_col != "uwi" and chunksize is lower than count
                single["index_col"] = orig_index_col

        rollups_dict = {}
        if "rollups" in args:
            for rollup in args["rollups"]:
                orig_index_col = rollup["index_col"]
                rollup_chunks = read_sql_table_chunked(
                    conn_str=conn_str,
                    table_name=rollup["table_name"],
                    index_col=rollup["index_col"],
                    subquery=rollup.get("subquery", None),
                    subconditions=rollup.get("subconditions", []),
                    conditions=rollup.get("conditions", []),
                    uwi_filter=args.get("uwi_filter", None),
                )
                rollup_chunk = next(rollup_chunks, pd.DataFrame())
                if not rollup_chunk.empty:
                    if rollup["index_col"] != "uwi":
                        rollup_chunk["uwi"] = rollup_chunk[rollup["index_col"]]
                        rollup["index_col"] = "uwi"
                    rollup_chunk.set_index(
                        rollup["index_col"], drop=False, inplace=True
                    )
                    rollup_chunk = rollup_chunk.rename(columns={"uwi": "uwi_link"})
                    group_by = rollup["group_by"]
                    r_agg = (
                        rollup_chunk.groupby(group_by)
                        .agg(lambda x: x.tolist())
                        .to_dict(orient="index")
                    )
                    rollups_dict[rollup["table_name"]] = r_agg

                # essential for when index_col != "uwi" and chunksize is lower than count
                rollup["index_col"] = orig_index_col

        for idx, row in primary_chunk.iterrows():
            record = {args["primary"]["table_name"]: row.to_dict()}
            for table_name, s_dict in singles_dict.items():
                record[table_name] = s_dict.get(idx, None)
            for table_name, r_dict in rollups_dict.items():
                record[table_name] = r_dict.get(idx, None)
            records.append(record)

    print(f"returning {len(records)} records", str(datetime.now()))
    return records


def main():
    ######################

    completion_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(uwi) FROM well_completion",
            "subconditions": ["__uwi_sub__"],
        },
        "rollups": [
            {
                "table_name": "well_completion",
                "index_col": "uwi",
                "group_by": "uwi",
            }
        ],
        # "uwi_filter": "050010902%",
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500%",
    }

    # Should be double-nesting based on well_core.core_id
    core_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(uwi) FROM well_core",
            "subconditions": ["__uwi_sub__"],
        },
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
        # "uwi_filter": "05057064630000",
        "uwi_filter": "0500%",
    }

    dst_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(uwi) FROM well_test",
            "subconditions": ["__uwi_sub__", "test_type = 'DST'"],
        },
        "rollups": [
            {
                "table_name": "well_test",
                "index_col": "uwi",
                "group_by": "uwi",
                "conditions": ["test_type = 'DST'"],
            },
            {
                "table_name": "well_test_pressure",
                "index_col": "uwi",
                "group_by": "uwi",
                "conditions": ["test_type = 'DST'"],
            },
            {
                "table_name": "well_test_recovery",
                "index_col": "uwi",
                "group_by": "uwi",
                "conditions": ["recovery_method = 'PIPE'"],
            },
            {
                "table_name": "well_test_flow",
                "index_col": "uwi",
                "group_by": "uwi",
                "conditions": ["test_type = 'DST'"],
            },
        ],
        "uwi_filter": "0500%",
        # "uwi_filter": "0500%",
    }

    formation_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(uwi) FROM well_formation",
            "subconditions": ["__uwi_sub__"],
        },
        "rollups": [
            {
                "table_name": "well_formation",
                "index_col": "uwi",
                "group_by": "uwi",
            }
        ],
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500%",
    }

    ip_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(uwi) FROM well_test",
            "subconditions": ["test_type = 'IP'", "__uwi_sub__"],
        },
        "rollups": [
            {
                "table_name": "well_test",
                "index_col": "uwi",
                "group_by": "uwi",
                "conditions": ["test_type = 'IP'"],
            },
            {
                "table_name": "well_test_pressure",
                "index_col": "uwi",
                "group_by": "uwi",
                "conditions": ["test_type = 'IP'"],
            },
            {
                "table_name": "well_test_recovery",
                "index_col": "uwi",
                "group_by": "uwi",
                "conditions": ["recovery_method = 'IP'"],
            },
            {
                "table_name": "well_test_flow",
                "index_col": "uwi",
                "group_by": "uwi",
                "conditions": ["test_type = 'IP'"],
            },
        ],
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500%",
    }

    perforation_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(uwi) FROM well_perforation",
            "subconditions": ["__uwi_sub__"],
        },
        "rollups": [
            {
                "table_name": "well_perforation",
                "index_col": "uwi",
                "group_by": "uwi",
            }
        ],
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500%",
    }

    # should be double-nesting based on well_cumulative_production.zone_id
    production_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(uwi) FROM well_cumulative_production",
            "subconditions": ["__uwi_sub__"],
        },
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
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500%",
    }

    # NOTE: we skip log_depth_cal_vec.vid = log_image_reg_log_section.log_depth_cal_vid
    raster_log_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(well_id) AS uwi FROM log_image_reg_log_section",
            "subconditions": ["__uwi_sub__"],
        },
        "rollups": [
            {
                "table_name": "log_image_reg_log_section",
                "index_col": "well_id",
                "group_by": "uwi",
            },
        ],
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500109%",
    }

    survey_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": (
                "SELECT DISTINCT(uwi) FROM well_dir_srvy_station"
                " UNION "
                "SELECT DISTINCT(uwi) FROM well_dir_proposed_srvy_station"
            ),
            "subconditions": ["__uwi_sub__"],
        },
        "singles": [
            {
                "table_name": "well_dir_srvy",
                "index_col": "uwi",
            },
            {
                "table_name": "well_dir_proposed_srvy",
                "index_col": "uwi",
            },
        ],
        "rollups": [
            {
                "table_name": "well_dir_srvy_station",
                "index_col": "uwi",
                "group_by": "uwi",
            },
            {
                "table_name": "well_dir_proposed_srvy_station",
                "index_col": "uwi",
                "group_by": "uwi",
            },
        ],
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500%",
    }

    vector_log_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT uwi FROM gx_well_curve",
            "subconditions": ["__uwi_sub__"],
        },
        "rollups": [
            {
                "table_name": "gx_well_curve",
                "index_col": "wellid",
                "group_by": "uwi",
            },
            {
                "table_name": "gx_well_curveset",
                "index_col": "wellid",
                "group_by": "uwi",
            },
            {
                "table_name": "gx_well_curve_values",
                "index_col": "wellid",
                "group_by": "uwi",
            },
        ],
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500%",
    }

    well_args = {
        "primary": {"table_name": "well", "index_col": "uwi"},
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        # "uwi_filter": "0500109%",
        "uwi_filter": "05001%",
    }

    zone_args = {
        "primary": {
            "table_name": "well",
            "index_col": "uwi",
            "subquery": "SELECT DISTINCT(uwi) FROM well_zone_interval",
            "subconditions": ["__uwi_sub__"],
        },
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
        # "uwi_filter": "050010902%",
        # "uwi_filter": "050010902%|050130657%|0504507834%",
        "uwi_filter": "0500%",
    }

    ######################

    records = collect_and_assemble_docs(completion_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(core_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(dst_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(formation_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(ip_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(perforation_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(production_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(raster_log_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(survey_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(vector_log_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(well_args)
    print('*****************************', len(records))
    records = collect_and_assemble_docs(zone_args)
    print('*****************************', len(records))

    # myjson = json.dumps(records, indent=4, cls=CustomEncoder)
    # print(myjson)
    # with open("output.txt", "w") as file:
    #     file.write(myjson)


if __name__ == "__main__":
    freeze_support()
    main()
