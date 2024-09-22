from typing import Dict, Union, List

from purr_geographix.assets.collect.xformer import PURR_WHERE


def make_where_clause(uwi_list: List[str]):
    """Construct the UWI-centric part of a WHERE clause containing UWIs. The
    WHERE clause will start: "WHERE 1=1 " to which we append:
    "AND u_uwi LIKE '0123%' OR u_uwi LIKE '4567'"

    Args:
        uwi_list (List[str]): List of UWI strings with optional wildcard chars
    """
    col = "w_uwi"
    clause = "WHERE 1=1"
    if uwi_list:
        uwis = [f"{col} LIKE '{uwi}'" for uwi in uwi_list]
        clause += " AND " + " OR ".join(uwis)

    return clause


def make_id_in_clauses(ids: Union[List[Union[str, int]], None]) -> str:
    """Generate a SQL WHERE clause for filtering by ids"""
    clause = "WHERE 1=1 "
    id_col = "w_uwi"

    if ids:
        quoted = ",".join(f"'{str(i)}'" for i in ids)
        clause += f"AND {id_col} IN ({quoted})"

    return clause


def create_selectors(
    chunked_ids: List[List[Union[str, int]]], recipe: Dict[str, str]
) -> List[str]:
    """Create a list of SQL selectors based on recipe and chunked ids"""
    selectors = []
    for ids in chunked_ids:
        # in_clause = make_id_in_clauses(recipe["identifier_keys"], chunk)
        in_clause = make_id_in_clauses(ids)
        select_sql = recipe["selector"].replace(PURR_WHERE, in_clause)
        selectors.append(select_sql)
    return selectors
