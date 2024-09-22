import pandas as pd
from typing import Any, List

pd.set_option("display.max_colwidth", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


def preserve_empty_lists(values: List[Any]) -> List[Any]:
    return [sublist if sublist is not None else [] for sublist in values]


def flexible_agg(
    df: pd.DataFrame, prefix_list: List[str], empty_list_cols: List[str] = []
) -> pd.DataFrame:
    """Convenience template for defining dataframe-to-json aggregation. This is
    used where aggregating in SQL alone is problematic. It's basically just a
    GROUP BY w_uwi with flexible handling of other table prefixes.
    (the empty_list_cols is used by Petra, just ignore it.)
    """

    def starts_with_any(col: str, prefixes: List[str]) -> bool:
        return any(col.startswith(prefix) for prefix in prefixes)

    agg_columns = [col for col in df.columns if starts_with_any(col, prefix_list)]

    agg_dict: dict[str, Any] = {}
    for col in agg_columns:
        if col in empty_list_cols:
            agg_dict[col] = preserve_empty_lists
        else:
            agg_dict[col] = list

    other_columns = [
        col for col in df.columns if col not in agg_columns and col != "w_uwi"
    ]
    for col in other_columns:
        agg_dict[col] = "first"

    return df.groupby("w_uwi", as_index=False).agg(agg_dict)


def ip_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["t_"],
        # empty_list_cols=["p_treat"],
    )


def production_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["p_", "m_"],
    )


def raster_log_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["v_", "r_"],
    )


def survey_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["s_", "d_"],
    )


def vector_log_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["c_", "s_", "v_"],
    )


def zone_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["i_", "v_", "z_"],
    )


post_process = {
    "ip_agg": ip_agg,
    "production_agg": production_agg,
    "raster_log_agg": raster_log_agg,
    "survey_agg": survey_agg,
    "vector_log_agg": vector_log_agg,
    "zone_agg": zone_agg,
}
