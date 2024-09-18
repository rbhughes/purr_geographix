"""GeoGraphix completion"""

from purr_geographix.assets.collect.xformer import PURR_DELIM, PURR_NULL, PURR_WHERE


selector = f"""
    WITH w AS (
        SELECT
            gx_wsn                       AS w_gx_wsn,
            uwi                          AS w_uwi,
            well_name                    AS w_well_name,
            well_number                  AS w_well_number,
            operator                     AS w_operator,
            lease_name                   AS w_lease_name,
            lease_number                 AS w_lease_number,
            county                       AS w_county,
            province_state               AS w_province_state,
            row_changed_date             AS w_row_changed_date
        FROM well
    ),
    c AS (
        SELECT
            uwi                             AS id_c_uwi,
            LIST(IFNULL(base_depth,         '{PURR_NULL}', CAST(base_depth AS VARCHAR)),         '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_base_depth,
            LIST(IFNULL(base_form,          '{PURR_NULL}', CAST(base_form  AS VARCHAR)),         '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_base_form,
            LIST(IFNULL(completion_date,    '{PURR_NULL}', CAST(completion_date  AS VARCHAR)),   '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_completion_date,
            LIST(IFNULL(completion_form,    '{PURR_NULL}', CAST(completion_form AS VARCHAR)),    '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_completion_form,
            LIST(IFNULL(completion_obs_no,  '{PURR_NULL}', CAST(completion_obs_no AS VARCHAR)),  '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_completion_obs_no,
            LIST(IFNULL(completion_type,    '{PURR_NULL}', CAST(completion_type AS VARCHAR)),    '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_completion_type,
            LIST(IFNULL(remark,             '{PURR_NULL}', CAST(remark AS VARCHAR)),             '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_remark,
            LIST(IFNULL(row_changed_date,   '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),   '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_row_changed_date,
            LIST(IFNULL(source,             '{PURR_NULL}', CAST(source AS VARCHAR)),             '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_source,
            LIST(IFNULL(top_depth,          '{PURR_NULL}', CAST(top_depth AS VARCHAR)),          '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_top_depth,
            LIST(IFNULL(top_form,           '{PURR_NULL}', CAST(top_form AS VARCHAR)),           '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_top_form,
            LIST(IFNULL(uwi,                '{PURR_NULL}', CAST(uwi AS VARCHAR)),                '{PURR_DELIM}' ORDER BY completion_obs_no)  AS c_uwi
        FROM well_completion
        GROUP BY uwi
    )
    SELECT
        w.*,
        c.*
    FROM w
    JOIN c ON w.w_uwi = c.id_c_uwi
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        DISTINCT c.uwi AS w_uwi
    FROM well_completion c
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "c_": "well_completion",
    },
    "xforms": {
        "c_base_depth": "array_of_float",
        "c_base_form": "array_of_string",
        "c_completion_date": "array_of_datetime",
        "c_completion_form": "array_of_string",
        "c_completion_obs_no": "array_of_int",
        "c_completion_type": "array_of_string",
        "c_remark": "array_of_string",
        "c_row_changed_date": "array_of_datetime",
        "c_source": "array_of_string",
        "c_top_depth": "array_of_float",
        "c_top_form": "array_of_float",
        "c_uwi": "array_of_string",
    },
    "chunk_size": 1000,
}
