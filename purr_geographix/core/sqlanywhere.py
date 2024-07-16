import pyodbc
import re
from pathlib import Path
from retry import retry
from typing import Any, Dict, List, Union
from purr_geographix.core.logger import logger


class RetryException(Exception):
    pass


SQLANY_DRIVER = "SQL Anywhere 17"


@retry(RetryException, tries=5)
def db_exec(conn: dict, sql: str) -> List[Dict[str, Any]] | Exception:
    """Convenience method for using pyodbc and SQLAnywhere with GeoGraphix

    Args:
        conn (dict): SQLAnywhere connection parameters.
        sql (str): A single SQL statement to execute on the gxdb.

    Returns:
        List[Dict[str, Any]]: list of dicts representing rows from query result.

    Raises:
        - RetryException: If the gxdb.db file is in use and the database name is
        not unique, trigger a retry without 'dbf' parameter.
        - pyodbc.ProgrammingError: For cases where table(s) might not exist due
        to an unxepected/ancient schema. Schema's >~ 2015 should work.
    """

    try:
        with pyodbc.connect(**conn) as connection:
            # connection.setencoding("CP1252")
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


def make_conn_params(repo_path: str, host: str) -> dict:
    """Assemble GeoGraphix-centric connection parameters used by pyodbc

    Args:
        repo_path (str): Path to a GeoGraphix project directory.
        host (Optional[str]): GeoGraphix project server hostname.

    Returns:
        dict: dictionary of SQLAnywhere connection parameters.
    """

    ggx_host = host or "localhost"
    fs_path = repo_path.replace("\\", "/")
    name = fs_path.split("/")[-1]
    home = fs_path.split("/")[-2]

    params = {
        "driver": SQLANY_DRIVER,
        "uid": "dba",
        "pwd": "sql",
        "host": ggx_host,
        "dbf": str(Path(fs_path) / "gxdb.db"),
        "dbn": name.replace(" ", "_") + "-" + home.replace(" ", "_"),
        "server": "GGX_" + ggx_host.upper(),
        "astart": "YES",
    }
    return params
