import json
import pyodbc
import pandas as pd
from datetime import datetime
from pathlib import Path
from purr_geographix.api_modules.database import get_db
from purr_geographix.api_modules.crud import get_repo_by_id, get_file_depot
from purr_geographix.assets.collect.select_templates import templates
from purr_geographix.common.util import (
    datetime_formatter,
    safe_numeric,
    timestamp_filename,
    CustomEncoder,
)

formatters = {
    "date": datetime_formatter(),
    "float": lambda x: safe_numeric(x),
    "int": lambda x: safe_numeric(x),
    "hex": lambda x: (
        x.hex() if isinstance(x, bytes) else (None if pd.isna(x) else str(x))
    ),
    "str": lambda x: str(x) if pd.notna(x) else "",
}


def build_where_clause(
        index_col="uwi",
        subquery=None,
        subconditions=None,
        conditions=None,
        uwi_query=None,
):
    clauses = []

    if subquery:
        clauses.append(
            build_subquery_clause(index_col, subquery, subconditions, uwi_query)
        )

    if conditions:
        clauses.extend(conditions)

    if uwi_query:
        clauses.append(f"{index_col} SIMILAR TO '{uwi_query}'")

    return f"WHERE {' AND '.join(clauses)}" if clauses else ""


def build_subquery_clause(index_col, subquery, subconditions, uwi_query):
    subz = []

    if subconditions:
        for sc in subconditions:
            if "__uwi_sub__" in sc and uwi_query:
                subz.append(f"{index_col} SIMILAR TO '{uwi_query}'")
            elif "__uwi_sub__" not in sc:
                subz.append(sc)

    sub_where = f" WHERE {' AND '.join(subz)}" if subz else ""
    return f"{index_col} IN ({subquery}{sub_where})"


##############################################################################


def read_sql_table_chunked(
        conn,
        uwi_query,
        table_name,
        index_col,
        subquery=None,
        subconditions=None,
        conditions=None,
        chunksize=10000,
):
    where_clause = build_where_clause(
        index_col, subquery, subconditions, conditions, uwi_query
    )

    def get_column_mappings():
        with pyodbc.connect(**conn) as cn:
            cursor = cn.cursor()

            # NOTE: columns set to lower()
            return {
                x.column_name.lower(): x.type_name
                for x in cursor.columns(table=table_name)
            }

    col_mappings = get_column_mappings()
    columns = list(col_mappings.keys())

    def read_chunk():

        start_at = 0
        while True:

            query = (
                f"SELECT TOP {chunksize} START AT {start_at + 1} * FROM "
                f"{table_name}  {where_clause}  ORDER BY {index_col} "
            )

            print(query)

            with pyodbc.connect(**conn) as cn:
                cursor = cn.cursor()
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


def collect_and_assemble_docs(args):
    conn = args["conn"]
    uwi_query = args.get("uwi_query", None)

    primary_chunks = read_sql_table_chunked(
        conn,
        uwi_query,
        table_name=args["primary"]["table_name"],
        index_col=args["primary"]["index_col"],
        subquery=args["primary"].get("subquery", None),
        subconditions=args["primary"].get("subconditions", []),
        conditions=args["primary"].get("conditions", []),
    )

    records = []
    for primary_chunk in primary_chunks:
        if primary_chunk.empty:
            continue

        primary_chunk.set_index(args["primary"]["index_col"], drop=False, inplace=True)
        print("MADE IT HERE 0")

        singles_dict = {}
        if "singles" in args:
            for single in args["singles"]:
                orig_index_col = single["index_col"]
                single_chunks = read_sql_table_chunked(
                    conn,
                    uwi_query,
                    table_name=single["table_name"],
                    index_col=single["index_col"],
                    subquery=single.get("subquery", None),
                    subconditions=single.get("subconditions", []),
                    conditions=single.get("conditions", []),
                )
                single_chunk = next(single_chunks, pd.DataFrame())
                if not single_chunk.empty:
                    if single["index_col"] != "uwi":
                        single_chunk["uwi"] = single_chunk[single["index_col"]]
                        single["index_col"] = "uwi"
                    single_chunk.set_index(
                        single["index_col"], drop=False, inplace=True
                    )
                    print("MADE IT HERE 1")
                    single_chunk = single_chunk.rename(columns={"uwi": "uwi_link"})
                    singles_dict[single["table_name"]] = single_chunk.to_dict(
                        orient="index"
                    )
                    print("MADE IT HERE 2")
                # essential for when index_col != "uwi" and chunksize is lower than count
                single["index_col"] = orig_index_col

        rollups_dict = {}
        if "rollups" in args:
            for rollup in args["rollups"]:
                orig_index_col = rollup["index_col"]
                rollup_chunks = read_sql_table_chunked(
                    conn,
                    uwi_query,
                    table_name=rollup["table_name"],
                    index_col=rollup["index_col"],
                    subquery=rollup.get("subquery", None),
                    subconditions=rollup.get("subconditions", []),
                    conditions=rollup.get("conditions", []),
                )
                rollup_chunk = next(rollup_chunks, pd.DataFrame())
                if not rollup_chunk.empty:
                    if rollup["index_col"] != "uwi":
                        rollup_chunk["uwi"] = rollup_chunk[rollup["index_col"]]
                        rollup["index_col"] = "uwi"
                    rollup_chunk.set_index(
                        rollup["index_col"], drop=False, inplace=True
                    )
                    print("MADE IT HERE 2")
                    rollup_chunk = rollup_chunk.rename(columns={"uwi": "uwi_link"})
                    group_by = rollup["group_by"]
                    r_agg = (
                        rollup_chunk.groupby(group_by)
                        .agg(lambda x: x.tolist())
                        .to_dict(orient="index")
                    )
                    rollups_dict[rollup["table_name"]] = r_agg

                # for when index_col != "uwi" and chunksize is lower than count
                rollup["index_col"] = orig_index_col

        for idx, row in primary_chunk.iterrows():
            record = {args["primary"]["table_name"]: row.to_dict()}

            record["purr_id"] = "_".join(
                [args["repo_id"], args["asset"], record["well"]["uwi"]]
            )

            for table_name, s_dict in singles_dict.items():
                record[table_name] = s_dict.get(idx, None)
            for table_name, r_dict in rollups_dict.items():
                record[table_name] = r_dict.get(idx, None)
            records.append(record)

    print(f"returning {len(records)} records", str(datetime.now()))
    return records


##############################################################################


def export_json(records, repo_id, asset):
    db = next(get_db())
    file_depot = Path(get_file_depot(db))

    jd = json.dumps(records, indent=4, cls=CustomEncoder)
    json_file = timestamp_filename(repo_id, asset)
    out_file = Path(file_depot / json_file)

    with open(out_file, "w") as file:
        file.write(jd)
    return f"Exported {len(records)} docs to: {out_file}"


# Want to send json directly? No you don't. See CustomJSONResponse in
# routes-assets, but this quickly clobbers browsers and is a generally bad idea.
async def selector(repo_id: str, asset: str, uwi_query: str = None):
    db = next(get_db())
    template = templates[asset]
    repo = get_repo_by_id(db, repo_id)
    conn = repo.conn

    collection_args = {
        **template,
        "repo_id": repo_id,
        "asset": asset,
        "conn": conn,
        "uwi_query": uwi_query,
    }
    records = collect_and_assemble_docs(collection_args)

    if len(records) > 0:
        summary = export_json(records, repo_id, asset)
        return summary

    else:
        return "Query returned no results"
