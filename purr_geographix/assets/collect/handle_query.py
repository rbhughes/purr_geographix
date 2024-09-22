"""GeoGraphix asset query"""

import json
from pathlib import Path
from typing import Any, Dict
import pandas as pd
import numpy as np
import pyodbc

from purr_geographix.core.sqlanywhere import db_exec
from purr_geographix.core.database import get_db
from purr_geographix.core.crud import get_repo_by_id, get_file_depot
from purr_geographix.assets.collect.xformer import formatters
from purr_geographix.core.util import async_wrap, import_dict_from_file
from purr_geographix.assets.collect.post_process import post_process
from purr_geographix.assets.collect.xformer import (
    PURR_WHERE,
    get_column_info,
    standardize_df_columns,
    transform_dataframe_to_json,
)
from purr_geographix.assets.collect.sql_helper import (
    make_where_clause,
    create_selectors,
)
# from purr_geographix.core.logger import logger


def chunk_ids(ids, chunk):
    """
    [621, 826, 831, 834, 835, 838, 846, 847, 848]
    ...with chunk=4...
    [[621, 826, 831, 834], [835, 838, 846, 847], [848]]

    ["1-62", "1-82", "2-83", "2-83", "2-83", "2-83", "2-84", "3-84", "4-84"]
    ...with chunk=4...
    [
        ['1-62', '1-82'],
        ['2-83', '2-83', '2-83', '2-83', '2-84'],
        ['3-84', '4-84']
    ]
    Note how the group of 2's is kept together, even if it exceeds chunk=4

    :param ids: This is usually a list of uwis. The ability to handle "compound"
        ids : ['1-11', '1-22', '1-33', '2-22', '2-44'] is leftover from Petra.
    :param chunk: The preferred batch size to process in a single query
    :return: List of id lists
    """
    id_groups = {}

    for item in ids:
        left = str(item).split("-", maxsplit=1)[0]
        if left not in id_groups:
            id_groups[left] = []
        id_groups[left].append(item)

    result = []
    current_subarray = []

    for group in id_groups.values():
        if len(current_subarray) + len(group) <= chunk:
            current_subarray.extend(group)
        else:
            result.append(current_subarray)
            current_subarray = group[:]

    if current_subarray:
        result.append(current_subarray)

    return result


def fetch_id_list(conn, id_sql):
    """Excutes an asset recipe's identifier SQL and returns ids"""
    res = db_exec(conn, id_sql)
    if res and "w_uwi" in res[0]:
        ids = [row["w_uwi"] for row in res]
        return ids
    else:
        return []


##############################################################################.


def collect_and_assemble_docs(args: Dict[str, Any]):
    """todo"""
    conn_params = args["conn"]
    recipe = args["recipe"]
    out_file = args["out_file"]
    xforms = recipe["xforms"]

    # control memory by the number of "ids" in the where clause of a selector
    chunk_size = recipe["chunk_size"] if "chunk_size" in recipe else 1000

    where = make_where_clause(args["uwi_list"])

    id_sql = recipe["identifier"].replace(PURR_WHERE, where)

    # print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
    # print(id_sql)
    # print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")

    ids = fetch_id_list(conn_params, id_sql)

    # print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
    # print(ids)
    # print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")

    chunked_ids = chunk_ids(ids, chunk_size)

    if len(chunked_ids) == 0:
        return "no hits"

    selectors = create_selectors(chunked_ids, recipe)

    # print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
    # print(selectors)
    # print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")

    all_columns = set()

    ######
    docs_written = 0
    ######

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("[")  # Start of JSON array

        for q in selectors:
            # print("qqqqqqqqqqqqqqqqqqqqqqqqqqq")
            # print(q)
            # print("qqqqqqqqqqqqqqqqqqqqqqqqqqq")

            # pylint: disable=c-extension-no-member
            with pyodbc.connect(**conn_params) as conn:
                cursor = conn.cursor()
                cursor.execute(q)

                column_names, column_types = get_column_info(cursor)

                df = pd.DataFrame(
                    [tuple(row) for row in cursor.fetchall()], columns=column_names
                )

                # duplicates = df[df.duplicated(subset=["w_uwi"])]
                # print("ddddddddddddddddddddddddddddd")
                # print(duplicates)

                df = standardize_df_columns(df, column_types)

                if not df.empty:
                    all_columns.update(df.columns)

                    for col in df.columns:
                        col_type = str(df.dtypes[col])

                        xform = xforms.get(col, col_type)

                        formatter = formatters.get(xform, lambda x: x)

                        # pylint: disable=cell-var-from-loop
                        df[col] = df[col].apply(formatter)

                    df = df.replace({np.nan: None})

                    if postproc := recipe.get("post_process"):
                        post_processor = post_process[postproc]
                        if post_processor:
                            print("post-processing", postproc)
                            df = post_processor(df)

                    # transform this chunk by table prefixes
                    json_data = transform_dataframe_to_json(df, recipe["prefixes"])

                    print("==========================>", len(json_data))

                    # TODO: test efficiency vs memory (maybe just send it all)
                    for json_obj in json_data:
                        json_str = json.dumps(json_obj, default=str)
                        f.write(json_str + ",")
                        docs_written += 1

        f.seek(f.tell() - 1, 0)  # Remove the last comma
        f.write("]")

    return {
        "message": f"{docs_written} docs written",
        "out_file": out_file,
    }


async def selector(
    repo_id: str, asset: str, export_file: str, uwi_list: str = None
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
    file_depot = get_file_depot(db)
    db.close()

    depot_path = Path(file_depot)
    out_file = Path(depot_path / export_file)

    if repo is None:
        return "Query returned no results"

    conn = repo.conn

    recipe_path = Path(Path(__file__).resolve().parent, f"recipes/{asset}.py")
    recipe = import_dict_from_file(recipe_path, "recipe")

    collection_args = {
        "recipe": recipe,
        "repo_id": repo_id,
        "conn": conn,
        "uwi_list": uwi_list,
        "out_file": out_file,
    }

    async_collect_and_assemble_docs = async_wrap(collect_and_assemble_docs)
    result = await async_collect_and_assemble_docs(collection_args)

    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    # # print(result)
    # # print("-------------------------------------------------")
    # with open(result["out_file"], "r") as file:
    #     data = json.load(file)
    #     print(json.dumps(data, indent=2))
    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    # return result  # sent to logger

    # # if len(records) > 0:
    # #     async_export_json = async_wrap(export_json)
    # #     return await async_export_json(records, export_file)
    # # else:
    # #     return "Query returned no results"
