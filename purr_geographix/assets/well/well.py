from purr_geographix.common.util import async_wrap
from purr_geographix.api_modules.database import get_db_instance
from purr_geographix.api_modules.crud import get_repo_by_id
from purr_geographix.common.sqlanywhere import db_exec

from typing import List

import pyodbc
import json


def sayit(s: str):
    print("SAY IT NOW", s)


sql = "SELECT * from well"


async def selector(repo_ids: List[str], query: str):
    print("SSSSSSSSSSSSSSSSSSS")
    print(repo_ids)
    print(query)

    db = get_db_instance()
    repo = get_repo_by_id(db, repo_ids[0])

    conn = pyodbc.connect(**repo.conn)
    cursor = conn.cursor

    def get_table_info():
        tables = {}
        for table_info in cursor.tables():
            if table_info.table_type == "TABLE":
                table_name = table_info.table_name
                tables[table_name] = {"columns": [], "primary_key": None}

                for column in cursor.columns(table=table_name):
                    column_name = column.column_name
                    tables[table_name]["columns"].append(column_name)

                    # Assume the first column is the primary key
                    if not tables[table_name]["primary_key"]:
                        tables[table_name]["primary_key"] = column_name

        return tables

        print(tables)

    tables = get_table_info()

    # for repo_id in repo_ids:
    #     # print("---->", repo_id)
    #     repo = get_repo_by_id(db, repo_id)
    #
    #     res = db_exec(repo.conn, sql)
    #     # print(res)
    #     print("-------------------------")
    #
    #     # repo = await get_repo_by_id(repo_id)

    db.close()
