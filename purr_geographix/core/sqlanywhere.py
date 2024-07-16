import pyodbc
import re
from pathlib import Path
from retry import retry
from typing import Any, Dict, List, Union
from purr_geographix.core.logger import logger


class RetryException(Exception):
    pass


SQLANY_DRIVER = "SQL Anywhere 17"


# SQLANY_DRIVER = "SQL Anywhere for Discovery"


@retry(RetryException, tries=5)
def db_exec(conn: dict, sql: str) -> List[Dict[str, Any]] | Exception:
    """
    This function connects to a SQLAnywhere database using the provided connection
    parameters and executes the given SQL statement(s). It returns the query results
    as a list of dictionaries or a list of lists of dictionaries.

    Args:
        conn (Union[dict, 'SQLAnywhereConn']): A dictionary or a SQLAnywhereConn object
            containing the connection parameters for the SQLAnywhere database.
        sql (Union[str, List[str]]): A single SQL statement or a list of SQL statements
            to execute on the database.

    Returns:
        Union[List[Dict[str, Any]], List[List[Dict[str, Any]]]]:
            - If `sql` is a single string, the function returns a list of dictionaries,
              where each dictionary represents a row in the query result, and the keys
              are the column names.
            - If `sql` is a list of strings, the function returns a list of lists of
              dictionaries, where each inner list represents the result of one SQL
              statement, and each dictionary represents a row in the query result.

    Raises:
        RetryException: If the gxdb.db file is in use and the database name is not unique,
            this exception is raised to allow retrying the connection with a different
            set of parameters.

    Notes:
        - If the `conn` parameter is a SQLAnywhereConn object, it is converted to a
          dictionary before establishing the connection.
        - If the gxdb.db file is in use and the database name is not unique, the `dbf`
          key is removed from the connection parameters, and a RetryException is raised.
          This allows retrying the connection with a different set of parameters, assuming
          the `dbn` parameter matches the name used by the process that has the gxdb opened.

    """

    try:
        with pyodbc.connect(**conn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)

                return [
                    dict(zip([col[0] for col in cursor.description], row))
                    for row in cursor.fetchall()
                ]

    except pyodbc.OperationalError as oe:
        logger.error({"error": oe, "context": conn})
        if re.search(r"Database name not unique", str(oe)):
            conn.pop("dbf")
            raise RetryException from oe
        else:
            return oe
    except pyodbc.ProgrammingError as pe:
        logger.error({"error": pe, "context": conn})
        if re.search(r"Table .* not found", str(pe)):
            return pe

    except Exception as ex:
        logger.error({"error": ex, "context": conn})
        raise ex


# @retry(RetryException, tries=5)
# def db_exec(
#     conn: Union[dict, "SQLAnywhereConn"], sql: Union[str, List[str]]
# ) -> Union[List[Dict[str, Any]], List[List[Dict[str, Any]]]]:
#     """
#     This function connects to a SQLAnywhere database using the provided connection
#     parameters and executes the given SQL statement(s). It returns the query results
#     as a list of dictionaries or a list of lists of dictionaries.
#
#     Args:
#         conn (Union[dict, 'SQLAnywhereConn']): A dictionary or a SQLAnywhereConn object
#             containing the connection parameters for the SQLAnywhere database.
#         sql (Union[str, List[str]]): A single SQL statement or a list of SQL statements
#             to execute on the database.
#
#     Returns:
#         Union[List[Dict[str, Any]], List[List[Dict[str, Any]]]]:
#             - If `sql` is a single string, the function returns a list of dictionaries,
#               where each dictionary represents a row in the query result, and the keys
#               are the column names.
#             - If `sql` is a list of strings, the function returns a list of lists of
#               dictionaries, where each inner list represents the result of one SQL
#               statement, and each dictionary represents a row in the query result.
#
#     Raises:
#         RetryException: If the gxdb.db file is in use and the database name is not unique,
#             this exception is raised to allow retrying the connection with a different
#             set of parameters.
#
#     Notes:
#         - If the `conn` parameter is a SQLAnywhereConn object, it is converted to a
#           dictionary before establishing the connection.
#         - If the gxdb.db file is in use and the database name is not unique, the `dbf`
#           key is removed from the connection parameters, and a RetryException is raised.
#           This allows retrying the connection with a different set of parameters, assuming
#           the `dbn` parameter matches the name used by the process that has the gxdb opened.
#
#     """
#
#     # if isinstance(conn, SQLAnywhereConn):
#     #     conn = conn.to_dict()
#
#     try:
#         with pyodbc.connect(**conn) as connection:
#             with connection.cursor() as cursor:
#                 if isinstance(sql, str):
#                     cursor.execute(sql)
#
#                     return [
#                         dict(zip([col[0] for col in cursor.description], row))
#                         for row in cursor.fetchall()
#                     ]
#
#                 if isinstance(sql, list):
#                     results = []
#                     for s in sql:
#                         cursor.execute(s)
#                         results.append(
#                             [
#                                 dict(zip([col[0] for col in cursor.description], row))
#                                 for row in cursor.fetchall()
#                             ]
#                         )
#                     return results
#
#     except pyodbc.OperationalError as oe:
#         if re.search(r"Database name not unique", str(oe)):
#             conn.pop("dbf")
#             raise RetryException from oe
#     except pyodbc.ProgrammingError as pe:
#         if re.search(r"Table .* not found", str(pe)):
#             return pe
#
#     except Exception as ex:
#         raise ex


def make_conn_params(repo_path: str, host: str) -> dict:
    """
    Args:
        repo_path (str): File path to the GeoGraphix project having a gxdb.db
        host (Optional[str]): Ideally, this is the project server's hostname
    Returns:
        dict: A dictionary containing SQLAnywhere connection parameters.
    """
    ggx_host = host or "localhost"
    fs_path = repo_path.replace("\\", "/")
    name = fs_path.split("/")[-1]
    home = fs_path.split("/")[-2]

    params = {
        # "driver": os.environ.get("SQLANY_DRIVER"),
        "driver": SQLANY_DRIVER,
        "uid": "dba",
        "pwd": "sql",
        "host": ggx_host,
        # "dbf": normalize_path(os.path.join(fs_path, "gxdb.db")),
        "dbf": str(Path(fs_path) / "gxdb.db"),
        "dbn": name.replace(" ", "_") + "-" + home.replace(" ", "_"),
        "server": "GGX_" + ggx_host.upper(),
        "astart": "YES",
    }
    return params
