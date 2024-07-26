"""GeoGraphix asset query overview.

Asset queries are well-centric. A parent well object is returned with every
asset type. Actual asset data is returned in 'rollups' of child records.
Sometimes this makes perfect sense, but occasionally a one-to-one
relationship is represented as a single-item array.

Why? Ideally, we would specify* exact columns and create more accurate joins
for each asset. However, schemas changes have resulted in new tables and
columns over the years, and that will likely continue. We take the pragmatic
approach and just join on UWI.

* Previous iterations of this utility used fully specified queries; contact
me if you want more details.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Generator
import pyodbc
import pandas as pd
from purr_geographix.core.database import get_db
from purr_geographix.core.crud import get_repo_by_id, get_file_depot
from purr_geographix.assets.collect.select_recipes import recipes
from purr_geographix.core.util import (
    async_wrap,
    datetime_formatter,
    safe_numeric,
    CustomJSONEncoder,
)
from purr_geographix.core.logger import logger


formatters = {
    "date": datetime_formatter(),
    "float": safe_numeric,
    "int": safe_numeric,
    "hex": lambda x: (
        x.hex() if isinstance(x, bytes) else (None if pd.isna(x) else str(x))
    ),
    "str": lambda x: str(x) if pd.notna(x) else "",
}


def build_where_clause(
    index_col: str = "uwi",
    subquery: str = None,
    subconditions: List[str] = None,
    conditions: List[str] = None,
    uwi_query: str = None,
):
    """Construct a WHERE clause based on a specific asset 'recipe'

    Recipes are dict templates that define primary and rollup table
    relationships. See select_recipes.py for specifics.

    Examples:
        (a complex example from survey, newlines added for clarity)
        WHERE uwi IN (
            SELECT DISTINCT(uwi) FROM well_dir_srvy_station
            UNION
            SELECT DISTINCT(uwi) FROM well_dir_proposed_srvy_station
            WHERE uwi SIMILAR TO '050570655%'
        )
        AND uwi SIMILAR TO '050570655%'

        (a simple example)
        WHERE uwi SIMILAR TO '050570655%'

    Args:
        index_col (str): The primary column name used for grouping, etc. This
            is usually 'uwi' or 'wellid'. Note that rollups/list aggregations
            are based only on uwi, not compound keys. This keeps things simple
            at the expense of possible duplication in child objects.
        subquery (str): A subquery used to limit parent (i.e. WELL) records
        subconditions (str): Additional limits (WHERE clause) for subquery.
        conditions (str): Top-level WHERE clause (just not in a subquery)
        uwi_query (str): A SIMILAR TO clause based on UWI string(s).

    Returns:
        str: Basically, a WHERE clause specific to this asset recipe.
    """
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


def build_subquery_clause(
    index_col: str,
    subquery: str,
    subconditions: List[str],
    uwi_query: str,
):
    """
    Examples:
        from survey (newlines added for clarity):

        "uwi IN (
            SELECT DISTINCT(uwi) FROM well_dir_srvy_station
            UNION
            SELECT DISTINCT(uwi) FROM well_dir_proposed_srvy_station
            WHERE uwi SIMILAR TO '050570655%'
        )"

    See build_where_clause for details
    """
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
    conn: Dict[str, any],
    uwi_query: str | None,
    portion: Dict[str, any],
    chunksize: int = 10000,
) -> Generator[pd.DataFrame, None, None]:
    """A generator to read chunks of data into DataFrames

    We use column datatypes as returned by pyodbc to apply some formatting and
    type checking, which prevents some None/NULL/<blank> issues in dataframes.

    Args:
        conn (dict): Connection parameters from repo.
        uwi_query (str): A SIMILAR TO clause based on UWI string(s).
        portion (dict): A dict defining parent/child relationships for queries
        chunksize (int): Maximum chunk for generator to yield

    Returns:
        Generator: a populated pd.DataFrame

    """
    table_name = portion["table_name"]
    index_col = portion["index_col"]
    subquery = portion.get("subquery", None)
    subconditions = portion.get("subconditions", [])
    conditions = portion.get("conditions", [])
    excluded_cols = portion.get("excluded_cols", [])

    where_clause = build_where_clause(
        index_col, subquery, subconditions, conditions, uwi_query
    )

    def get_column_mappings() -> Dict[str, str]:
        """Get column names and (odbc-centric) datatypes from pyodbc

        Returns:
            Dict[str, str]: lower_case column names and datatypes
        """
        # pylint: disable=c-extension-no-member
        with pyodbc.connect(**conn) as cn:
            cursor = cn.cursor()

            return {
                x.column_name.lower(): x.type_name
                for x in cursor.columns(table=table_name)
                if x.column_name.lower() not in excluded_cols
            }

    col_mappings = get_column_mappings()
    columns = list(col_mappings.keys())

    def read_chunk():
        start_at = 0
        while True:
            query = (
                f"SELECT TOP {chunksize} START AT {start_at + 1} "
                f"{",".join(columns)} FROM {table_name} "
                f"{where_clause} ORDER BY {index_col} "
            )

            logger.debug(query)

            # pylint: disable=c-extension-no-member
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

            if df.empty:
                break
            yield df
            start_at += chunksize

    return read_chunk()


def collect_and_assemble_docs(args: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Execute SQL queries and combine results into JSON documents

    Args:
        args (Dict[str, Any]: Contains connection params, a UWI filter and a
        specific 'recipe' for how to query and merge the results into JSON

    Returns:
        List[Dict[str, Any]]: A list of well-centric documents
    """
    conn = args["conn"]
    uwi_query = args.get("uwi_query", None)
    recipe = args["recipe"]

    primary_chunks = read_sql_table_chunked(
        conn,
        uwi_query,
        portion=recipe["primary"],
    )

    records = []
    for primary_chunk in primary_chunks:
        if primary_chunk.empty:
            continue

        primary_chunk.set_index(
            recipe["primary"]["index_col"], drop=False, inplace=True
        )

        singles_dict = {}
        if "singles" in recipe:
            for single in recipe["singles"]:
                orig_index_col = single["index_col"]
                single_chunks = read_sql_table_chunked(conn, uwi_query, portion=single)
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
        if "rollups" in recipe:
            for rollup in recipe["rollups"]:
                orig_index_col = rollup["index_col"]
                rollup_chunks = read_sql_table_chunked(
                    conn,
                    uwi_query,
                    portion=rollup,
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

                # when index_col != "uwi" and chunksize is lower than count
                rollup["index_col"] = orig_index_col

        for idx, row in primary_chunk.iterrows():
            record = {recipe["primary"]["table_name"]: row.to_dict()}

            record["purr_id"] = "_".join(
                [args["repo_id"], args["asset"], record["well"]["uwi"]]
            )

            for table_name, s_dict in singles_dict.items():
                record[table_name] = s_dict.get(idx, None)

            for table_name, r_dict in rollups_dict.items():
                record[table_name] = r_dict.get(idx, None)
            records.append(record)

    logger.info(f"returning {len(records)} records")
    return records


def export_json(records, export_file) -> str:
    """Convert dicts to JSON and save the file.

    Args:
        records (List[Dict[str, Any]]): The list of dicts obtained by
        collect_and_assemble_docs.
        export_file (str): The timestamp export file name defined earlier

    Returns:
        str: A summary containing counts and file path

    TODO: Investigate streaming?
    """
    db = next(get_db())
    file_depot = get_file_depot(db)
    db.close()
    depot_path = Path(file_depot)

    jd = json.dumps(records, indent=4, cls=CustomJSONEncoder)
    out_file = Path(depot_path / export_file)

    with open(out_file, "w", encoding="utf-8") as file:
        file.write(jd)

    return f"Exported {len(records)} docs to: {out_file}"


async def selector(
    repo_id: str, asset: str, export_file: str, uwi_query: str = None
) -> str:
    """Main entry point to collect data from a GeoGraphix project

    Args:
        repo_id (str): ID from a specific GeoGraphix project
        asset (str): An asset (i.e. datatype) to query from a gxdb
        export_file (str): Export file name with timestamp
        uwi_query (str): A SIMILAR TO clause based on UWI string(s).

    Returns:
        str: A summary of the selector job--probably from export_json()
    """
    db = next(get_db())
    repo = get_repo_by_id(db, repo_id)
    db.close()

    if repo is None:
        return "Query returned no results"

    conn = repo.conn

    collection_args = {
        "recipe": recipes[asset],
        "repo_id": repo_id,
        "asset": asset,
        "conn": conn,
        "uwi_query": uwi_query,
    }

    async_collect_and_assemble_docs = async_wrap(collect_and_assemble_docs)
    records = await async_collect_and_assemble_docs(collection_args)

    if len(records) > 0:
        async_export_json = async_wrap(export_json)
        return await async_export_json(records, export_file)
    else:
        return "Query returned no results"
