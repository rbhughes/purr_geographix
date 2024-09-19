"""GeoGraphix perforation"""

from purr_geographix.assets.collect.xformer import PURR_DELIM, PURR_NULL, PURR_WHERE


selector = f"""
    WITH w AS (
        SELECT
            gx_wsn            AS w_gx_wsn,
            uwi               AS w_uwi,
            well_name         AS w_well_name,
            well_number       AS w_well_number,
            operator          AS w_operator,
            lease_name        AS w_lease_name,
            lease_number      AS w_lease_number,
            county            AS w_county,
            province_state    AS w_province_state,
            row_changed_date  AS w_row_changed_date
        FROM well
    ),
    p AS (
        SELECT 
            uwi                                     AS id_p_uwi,
            MAX(row_changed_date)                   AS max_row_changed_date,
            LIST(IFNULL(base_depth,                 '{PURR_NULL}',  CAST(base_depth AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_base_depth,
            LIST(IFNULL(base_form,                  '{PURR_NULL}',  CAST(base_form AS VARCHAR)),                  '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_base_form,
            LIST(IFNULL(cluster,                    '{PURR_NULL}',  CAST(cluster AS VARCHAR)),                    '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_cluster,
            LIST(IFNULL(completion_obs_no,          '{PURR_NULL}',  CAST(completion_obs_no AS VARCHAR)),          '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_completion_obs_no,
            LIST(IFNULL(completion_source,          '{PURR_NULL}',  CAST(completion_source AS VARCHAR)),          '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_completion_source,
            LIST(IFNULL(current_status,             '{PURR_NULL}',  CAST(current_status AS VARCHAR)),             '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_current_status,
            LIST(IFNULL(gx_base_form_alias,         '{PURR_NULL}',  CAST(gx_base_form_alias AS VARCHAR)),         '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_gx_base_form_alias,
            LIST(IFNULL(gx_top_form_alias,          '{PURR_NULL}',  CAST(gx_top_form_alias AS VARCHAR)),          '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_gx_top_form_alias,
            LIST(IFNULL(perforation_angle,          '{PURR_NULL}',  CAST(perforation_angle AS VARCHAR)),          '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_angle,
            LIST(IFNULL(perforation_count,          '{PURR_NULL}',  CAST(perforation_count AS VARCHAR)),          '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_count,
            LIST(IFNULL(perforation_date,           '{PURR_NULL}',  CAST(perforation_date AS VARCHAR)),           '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_date,
            LIST(IFNULL(perforation_density,        '{PURR_NULL}',  CAST(perforation_density AS VARCHAR)),        '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_density,
            LIST(IFNULL(perforation_diameter,       '{PURR_NULL}',  CAST(perforation_diameter AS VARCHAR)),       '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_diameter,
            LIST(IFNULL(perforation_diameter_ouom,  '{PURR_NULL}',  CAST(perforation_diameter_ouom AS VARCHAR)),  '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_diameter_ouom,
            LIST(IFNULL(perforation_obs_no,         '{PURR_NULL}',  CAST(perforation_obs_no AS VARCHAR)),         '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_obs_no,
            LIST(IFNULL(perforation_per_uom,        '{PURR_NULL}',  CAST(perforation_per_uom AS VARCHAR)),        '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_per_uom,
            LIST(IFNULL(perforation_phase,          '{PURR_NULL}',  CAST(perforation_phase AS VARCHAR)),          '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_phase,
            LIST(IFNULL(perforation_type,           '{PURR_NULL}',  CAST(perforation_type AS VARCHAR)),           '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_perforation_type,
            LIST(IFNULL(remark,                     '{PURR_NULL}',  CAST(remark AS VARCHAR)),                     '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_remark,
            LIST(IFNULL(row_changed_date,           '{PURR_NULL}',  CAST(row_changed_date AS VARCHAR)),           '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_row_changed_date,
            LIST(IFNULL(source,                     '{PURR_NULL}',  CAST(source AS VARCHAR)),                     '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_source,
            LIST(IFNULL(stage,                      '{PURR_NULL}',  CAST(stage AS VARCHAR)),                      '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_stage,
            LIST(IFNULL(top_depth,                  '{PURR_NULL}',  CAST(top_depth AS VARCHAR)),                  '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_top_depth,
            LIST(IFNULL(top_form,                   '{PURR_NULL}',  CAST(top_form AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_top_form,
            LIST(IFNULL(uwi,                        '{PURR_NULL}',  CAST(uwi AS VARCHAR)),                        '{PURR_DELIM}' ORDER BY perforation_obs_no) AS p_uwi
        FROM well_perforation
        GROUP BY uwi
    )
    SELECT
        w.*,
        p.*
    FROM w
    JOIN p ON w.w_uwi = p.id_p_uwi
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        DISTINCT p.uwi AS w_uwi
    FROM well_perforation p
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "p_": "well_perforation",
    },
    "xforms": {
        "p_base_depth": "array_of_float",
        "p_base_form": "array_of_string",
        "p_cluster": "array_of_string",
        "p_completion_obs_no": "array_of_int",
        "p_completion_source": "array_of_string",
        "p_current_status": "array_of_string",
        "p_gx_base_form_alias": "array_of_string",
        "p_gx_top_form_alias": "array_of_string",
        "p_perforation_angle": "array_of_float",
        "p_perforation_count": "array_of_int",
        "p_perforation_date": "array_of_datetime",
        "p_perforation_density": "array_of_float",
        "p_perforation_diameter": "array_of_float",
        "p_perforation_diameter_ouom": "array_of_string",
        "p_perforation_obs_no": "array_of_int",
        "p_perforation_per_uom": "array_of_string",
        "p_perforation_phase": "array_of_string",
        "p_perforation_type": "array_of_string",
        "p_remark": "array_of_string",
        "p_row_changed_date": "array_of_datetime",
        "p_source": "array_of_string",
        "p_stage": "array_of_string",
        "p_top_depth": "array_of_float",
        "p_top_form": "array_of_string",
        "p_uwi": "array_of_string",
    },
    "chunk_size": 1000,
}
