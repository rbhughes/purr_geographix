"""GeoGraphix zone"""

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
    i AS (
        SELECT
        base_md                    AS i_base_md,
        base_tvd                   AS i_base_tvd,
        base_x                     AS i_base_x,
        base_y                     AS i_base_y,
        md                         AS i_md,
        mdss                       AS i_mdss,
        remark                     AS i_remark,
        row_changed_date           AS i_row_changed_date,
        --source                     AS i_source,
        top_md                     AS i_top_md,
        top_tvd                    AS i_top_tvd,
        top_x                      AS i_top_x,
        top_y                      AS i_top_y,
        tvd                        AS i_tvd,
        tvdss                      AS i_tvdss,
        uwi                        AS i_uwi,
        x                          AS i_x,
        y                          AS i_y,
        --zone_id                    AS i_zone_id,
        zone_name                  AS i_zone_name
        FROM well_zone_interval
    ),
    v AS (
        SELECT 
        v.uwi                      AS j_uwi,
        v.zone_name                AS j_zone_name, 
        MAX(v.row_changed_date)    AS max_row_changed_date,
        LIST(IFNULL(v.gx_remark,                '{PURR_NULL}', v.gx_remark),                                 '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS v_gx_remark,
        LIST(IFNULL(v.row_changed_date,         '{PURR_NULL}', v.row_changed_date),                          '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS v_row_changed_date,
        LIST(IFNULL(v.uwi,                      '{PURR_NULL}', v.uwi),                                       '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS v_uwi,
        LIST(IFNULL(v.zattribute_name,          '{PURR_NULL}', v.zattribute_name),                           '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS v_zattribute_name,
        LIST(IFNULL(v.zattribute_value_date,    '{PURR_NULL}', CAST(v.zattribute_value_date AS VARCHAR)),    '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS v_zattribute_value_date,
        LIST(IFNULL(v.zattribute_value_numeric, '{PURR_NULL}', CAST(v.zattribute_value_numeric AS VARCHAR)), '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS v_zattribute_value_numeric,
        LIST(IFNULL(v.zattribute_value_string,  '{PURR_NULL}', v.zattribute_value_string),                   '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS v_zattribute_value_string,
        LIST(IFNULL(v.zone_name,                '{PURR_NULL}', v.zone_name),                                 '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS v_zone_name,
        LIST(IFNULL(a.gx_remark,                '{PURR_NULL}', a.gx_remark),                                 '{PURR_DELIM}')                             AS a_gx_remark,
        LIST(IFNULL(a.row_changed_date,         '{PURR_NULL}', CAST(a.row_changed_date AS VARCHAR)),         '{PURR_DELIM}')                             AS a_row_changed_date,
        LIST(IFNULL(a.zattribute_name,          '{PURR_NULL}', a.zattribute_name),                           '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS a_zattribute_name,
        LIST(IFNULL(a.zone_name,                '{PURR_NULL}', a.zone_name),                                 '{PURR_DELIM}' ORDER BY v.zattribute_name)  AS a_zone_name,
        LIST(IFNULL(g.domain,                   '{PURR_NULL}', g.domain),                                    '{PURR_DELIM}')                             AS g_domain,
        LIST(IFNULL(g.gx_remark,                '{PURR_NULL}', g.gx_remark),                                 '{PURR_DELIM}')                             AS g_gx_remark,
        LIST(IFNULL(g.row_changed_date,         '{PURR_NULL}', CAST(g.row_changed_date AS VARCHAR)),         '{PURR_DELIM}')                             AS g_row_changed_date,
        LIST(IFNULL(g.zattribute_decimals,      '{PURR_NULL}', CAST(g.zattribute_decimals AS VARCHAR)),      '{PURR_DELIM}')                             AS g_zattribute_decimals,
        LIST(IFNULL(g.zattribute_name,          '{PURR_NULL}', g.zattribute_name),                           '{PURR_DELIM}')                             AS g_zattribute_name,
        LIST(IFNULL(g.zattribute_type,          '{PURR_NULL}', CAST(g.zattribute_type AS VARCHAR)),          '{PURR_DELIM}')                             AS g_zattribute_type,
        LIST(IFNULL(g.zattribute_value_unit,    '{PURR_NULL}', g.zattribute_value_unit),                     '{PURR_DELIM}')                             AS g_zattribute_value_unit
        FROM well_zone_intrvl_value v
        JOIN gx_zone_zattribute a ON v.zone_name = a.zone_name AND v.zattribute_name = a.zattribute_name
        JOIN gx_zattribute g ON a.zattribute_name = g.zattribute_name
        GROUP BY j_uwi, j_zone_name
    ),
    z AS (
        SELECT
        domain                     AS z_domain,
        gx_base_form_name          AS z_gx_base_form_name,
        gx_base_form_tb            AS z_gx_base_form_tb,
        gx_base_modifier           AS z_gx_base_modifier,
        gx_base_offset             AS z_gx_base_offset,
        gx_remark                  AS z_gx_remark,
        gx_top_form_name           AS z_gx_top_form_name,
        gx_top_form_tb             AS z_gx_top_form_tb,
        gx_top_modifier            AS z_gx_top_modifier,
        gx_top_offset              AS z_gx_top_offset,
        gx_topmidbase              AS z_gx_topmidbase,
        row_changed_date           AS z_row_changed_date,
        zone_name                  AS z_zone_name
        FROM gx_zone
    )
    SELECT
        w.*,
        i.*,
        v.*,
        z.*
    FROM w
    JOIN i ON i.i_uwi = w.w_uwi
    FULL OUTER JOIN v ON v.j_uwi = i.i_uwi  AND i.i_zone_name = v.j_zone_name
    JOIN z ON i.i_zone_name = z.z_zone_name
    {PURR_WHERE}
    """


# NOTE: not joined to well
identifier = f"""
    SELECT
        DISTINCT i.uwi AS w_uwi
    FROM well_zone_interval i
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "i_": "well_zone_interval",
        "v_": "well_zone_intrvl_value",
        "z_": "gx_zone",
        "a_": "gx_zone_zattribute",
        "g_": "gx_zattribute",
    },
    "xforms": {
        "v_gx_remark": "array_of_string",
        "v_row_changed_date": "array_of_datetime",
        "v_uwi": "array_of_string",
        "v_zattribute_name": "array_of_string",
        "v_zattribute_value_date": "array_of_datetime",
        "v_zattribute_value_numeric": "array_of_float",
        "v_zattribute_value_string": "array_of_string",
        "v_zone_name": "array_of_string",
        "a_gx_remark": "array_of_string",
        "a_row_changed_date": "array_of_datetime",
        "a_zattribute_name": "array_of_string",
        "a_zone_name": "array_of_string",
        "g_domain": "array_of_string",
        "g_gx_remark": "array_of_string",
        "g_row_changed_date": "array_of_datetime",
        "g_zattribute_decimals": "array_of_int",
        "g_zattribute_name": "array_of_string",
        "g_zattribute_type": "array_of_int",
        "g_zattribute_value_unit": "array_of_string",
    },
    "post_process": "zone_agg",
    "chunk_size": 1000,
}
