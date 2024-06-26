import pyodbc
from typing import Any, Dict, List, Union


def process_child(cursor, child_table, prefix):
    col_mappings = {
        x.column_name.lower(): (x.type_name, x.column_size)
        for x in cursor.columns(table=child_table)
    }

    # add primary keys
    # pk = [x.column_name.lower() for x in cursor.primaryKeys(table=child_table)]
    # cols = [f"{p} AS {table_name}__{p}" for p in primary_keys]
    # print(cols)

    cols = [f"{n} AS {prefix}_{n}" for n in col_mappings.keys()]
    # sql = f"SELECT {", ".join(cols)} FROM {child_table}"

    ####
    sql = "SELECT\n"
    sql += ",\n".join([c for c in cols]) + "\n"
    sql += f"FROM {child_table}"
    # sql += f"GROUP BY {", ".join(child_fk)}"

    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@")
    # print(sql)
    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@")

    return sql


def process_grandchild(cursor, grandchild_table, child_table, prefix):
    purr_null = "__purrNULL__"
    purr_delimiter = "__purrDELIMITER__"

    col_mappings = {
        x.column_name.lower(): (x.type_name, x.column_size)
        for x in cursor.columns(table=grandchild_table)
    }

    # pk = [x.column_name.lower() for x in cursor.primaryKeys(table=grandchild_table)]
    # print(f"grandCHILD pK: {pk}")

    if child_table:
        child_fk = set(
            [x.fkcolumn_name.lower() for x in cursor.foreignKeys(table=child_table)]
        )
    else:
        child_fk = set()
    ##############
    child_fk.add("uwi")
    # print(f"CHILD_TABLE={child_table}")
    # # ttt = set([x.fkcolumn_name.lower() for x in cursor.foreignKeys(table=child_table)])
    # ttt = set([x for x in cursor.foreignKeys(table=child_table)])
    # print(ttt)
    ##############

    # add child foreign keys as single-column
    cols = [f"{n} AS {prefix}_{n}" for n in child_fk]

    # add grandchild columns
    for col in sorted(cursor.columns(table=grandchild_table)):
        column_name = col.column_name.lower()
        type_name, column_size = col_mappings[column_name]

        # skip, we already added these as child fk
        if column_name in child_fk:
            continue

        if type_name in ["integer", "numeric", "double", "timestamp"]:
            cols.append(
                f"LIST(COALESCE(CAST({column_name} AS VARCHAR({column_size})), "
                f"'{purr_null}'), '{purr_delimiter}') AS {prefix}_{column_name}"
            )
        else:
            cols.append(
                f"LIST(COALESCE({column_name}, '{purr_null}'),  "
                f"'{purr_delimiter}') AS {prefix}__{column_name}"
            )

    sql = "SELECT\n"
    sql += ",\n".join([c for c in cols]) + "\n"
    sql += f"FROM {grandchild_table}\n"
    sql += f"GROUP BY {", ".join(child_fk)}"

    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@")
    # print(sql)
    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@")

    return sql


def blocky(conn: dict, child_tables=[], grandchild_tables=[]):
    print(f"GRANDCHILD_TABLE={grandchild_tables}   CHILD_TABLE={child_tables}")
    try:
        with pyodbc.connect(**conn) as connection:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM ( WITH \n"
                cte = ["w AS (SELECT uwi FROM well)"]
                i = 97
                alias = set()

                for child_table in child_tables:
                    ct = process_child(cursor, child_table, prefix=chr(i))

                    cte.append(f"{chr(i)} AS ({ct})")
                    alias.add(chr(i))
                    i += 1

                print("==============")

                for grandchild_table in grandchild_tables:
                    gct = process_grandchild(
                        cursor, grandchild_table, child_table=None, prefix=chr(i)
                    )

                    cte.append(f"{chr(i)} AS ({gct})")
                    alias.add(chr(i))
                    i += 1

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

            sql += ",\n".join([s for s in cte]) + "\n"
            sql += f"SELECT \n"
            sql += ",\n".join([f"{a}.*" for a in alias]) + "\n"
            sql += "FROM w\n"
            sql += "\n".join([f"JOIN {a} ON {a}.{a}_uwi = w.uwi" for a in alias]) + "\n"
            sql += "--where here\n"
            sql += ") purr"

            print(sql)

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

# res = blocky(
#     conn=repo_conn,
#     child_tables=["well_dir_srvy"],
#     grandchild_tables=["well_dir_srvy_station"],
# )

# res = blocky(
#     conn=repo_conn,
#     # child_tables=["well_formation"],
#     grandchild_tables=["well_formation"],
# )

# res = blocky(
#     conn=repo_conn,
#     child_tables=["well_dir_srvy", "well_dir_proposed_srvy"],
#     grandchild_tables=["well_dir_srvy_station", "well_dir_proposed_srvy_station"],
# )

res = blocky(
    conn=repo_conn,
    child_tables=["well"],
    # grandchild_tables=["well_formation"],
)
