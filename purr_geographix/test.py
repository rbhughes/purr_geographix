import pyodbc
from typing import Any, Dict, List, Union


def process_child(cursor, table_name):
    child_col_mappings = {
        x.column_name.lower(): (x.type_name, x.column_size)
        for x in cursor.columns(table=table_name)
    }
    cols = [f"{n} AS {table_name}__{n}\n" for n in child_col_mappings.keys()]
    sql = f"SELECT {", ".join(cols)} FROM {table_name}"

    child_pk = [x.column_name for x in cursor.primaryKeys(table=table_name)]

    # print(sql)
    return {"sql": sql, "child_pk": child_pk}


def process_grandchild(cursor, table_name):
    purr_null = "__purrNULL__"
    purr_delimiter = "__purrDELIMITER__"

    grandchild_col_mappings = {
        x.column_name.lower(): (x.type_name, x.column_size)
        for x in cursor.columns(table=table_name)
    }

    print(grandchild_col_mappings)

    sql = []
    for col in cursor.columns(table=table_name):
        column_name = col.column_name.lower()
        type_name, column_size = grandchild_col_mappings[column_name]
        if type_name in ["numeric", "double", "timestamp"]:
            sql.append(
                f"LIST(COALESCE(CAST({column_name} AS VARCHAR({column_size})), "
                f"'{purr_null}'), '{purr_delimiter}') AS z_{column_name}"
            )
        else:
            sql.append(
                f"LIST(COALESCE({column_name}, '{purr_null}'),  "
                f"'{purr_delimiter}') AS z_{column_name}"
            )

        # print(type_name, column_size)
    for s in sql:
        print(s, ", ")

    # grandchild_cols = [
    #     f"{x.column_name.lower()} AS a_{x.column_name}\n"
    #     for x in cursor.columns(table=table_name)
    # ]
    #
    # sql = f"SELECT {", ".join([n for n in grandchild_cols])} FROM {table_name}"
    # print(sql)
    #
    # grandchild_pk = [x.column_name for x in cursor.primaryKeys(table=table_name)]
    # print(grandchild_pk)


def blocky(conn: dict, child_tables, grandchild_tables):
    try:
        with pyodbc.connect(**conn) as connection:
            with connection.cursor() as cursor:
                # cursor.execute(sql)

                for child_table in child_tables:
                    process_child(cursor, child_table)

                print("==============")

                for grandchild_table in grandchild_tables:
                    process_grandchild(cursor, grandchild_table)

                # c = cursor.columns(table="well_dir_srvy")
                # print([a.column_name for a in c])
                # print("....")
                #
                # x = cursor.primaryKeys(table="well_dir_srvy")
                # print([f"{a.table_name}.{a.column_name}" for a in x])
                # print("....")
                #
                # y = cursor.foreignKeys(table="well_dir_srvy")
                # print([f"{a.fktable_name}.{a.fkcolumn_name}" for a in y])

    except Exception as ex:
        raise ex


repo_conn = {
    "astart": "YES",
    "dbf": "\\\\scarab\\ggx_projects\\Colorado_North\\gxdb.db",
    "dbn": "Colorado_North-ggx_projects",
    "driver": "SQL Anywhere 17",
    "host": "scarab",
    "pwd": "sql",
    "server": "GGX_SCARAB",
    "uid": "dba",
    "port": None,
}

res = blocky(
    conn=repo_conn,
    child_tables=["well_dir_srvy"],
    grandchild_tables=["well_dir_srvy_station"],
)
# print(res)
