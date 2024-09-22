# pylint: disable=line-too-long
"""GeoGraphix formation"""

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
    f AS (
        SELECT
            uwi                                   AS id_f_uwi,
            MAX(row_changed_date)                 AS max_row_changed_date,
            LIST(IFNULL(dominant_lithology,       '{PURR_NULL}', CAST(dominant_lithology AS VARCHAR)),       '{PURR_DELIM}' ORDER BY form_obs_no) AS f_dominant_lithology,
            LIST(IFNULL(fault_name,               '{PURR_NULL}', CAST(fault_name AS VARCHAR)),               '{PURR_DELIM}' ORDER BY form_obs_no) AS f_fault_name,
            LIST(IFNULL(faulted_ind,              '{PURR_NULL}', CAST(faulted_ind AS VARCHAR)),              '{PURR_DELIM}' ORDER BY form_obs_no) AS f_faulted_ind,
            LIST(IFNULL(form_depth,               '{PURR_NULL}', CAST(form_depth AS VARCHAR)),               '{PURR_DELIM}' ORDER BY form_obs_no) AS f_form_depth,
            LIST(IFNULL(form_id,                  '{PURR_NULL}', CAST(form_id AS VARCHAR)),                  '{PURR_DELIM}' ORDER BY form_obs_no) AS f_form_id,
            LIST(IFNULL(form_obs_no,              '{PURR_NULL}', CAST(form_obs_no AS VARCHAR)),              '{PURR_DELIM}' ORDER BY form_obs_no) AS f_form_obs_no,
            LIST(IFNULL(form_tvd,                 '{PURR_NULL}', CAST(form_tvd AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY form_obs_no) AS f_form_tvd,
            LIST(IFNULL(gap_thickness,            '{PURR_NULL}', CAST(gap_thickness AS VARCHAR)),            '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gap_thickness,
            LIST(IFNULL(gx_base_subsea,           '{PURR_NULL}', CAST(gx_base_subsea AS VARCHAR)),           '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_base_subsea,
            LIST(IFNULL(gx_dip,                   '{PURR_NULL}', CAST(gx_dip AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_dip,
            LIST(IFNULL(gx_dip_azimuth,           '{PURR_NULL}', CAST(gx_dip_azimuth AS VARCHAR)),           '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_dip_azimuth,
            LIST(IFNULL(gx_eroded_ind,            '{PURR_NULL}', CAST(gx_eroded_ind AS VARCHAR)),            '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_eroded_ind,
            LIST(IFNULL(gx_exten_id,              '{PURR_NULL}', CAST(gx_exten_id AS VARCHAR)),              '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_exten_id,
            LIST(IFNULL(gx_form_base_depth,       '{PURR_NULL}', CAST(gx_form_base_depth AS VARCHAR)),       '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_form_base_depth,
            LIST(IFNULL(gx_form_base_tvd,         '{PURR_NULL}', CAST(gx_form_base_tvd AS VARCHAR)),         '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_form_base_tvd,
            LIST(IFNULL(gx_form_depth_datum,      '{PURR_NULL}', CAST(gx_form_depth_datum AS VARCHAR)),      '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_form_depth_datum,
            LIST(IFNULL(gx_form_id_alias,         '{PURR_NULL}', CAST(gx_form_id_alias AS VARCHAR)),         '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_form_id_alias,
            LIST(IFNULL(gx_form_top_depth,        '{PURR_NULL}', CAST(gx_form_top_depth AS VARCHAR)),        '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_form_top_depth,
            LIST(IFNULL(gx_form_top_tvd,          '{PURR_NULL}', CAST(gx_form_top_tvd AS VARCHAR)),          '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_form_top_tvd,
            LIST(IFNULL(gx_form_x_coordinate,     '{PURR_NULL}', CAST(gx_form_x_coordinate AS VARCHAR)),     '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_form_x_coordinate,
            LIST(IFNULL(gx_form_y_coordinate,     '{PURR_NULL}', CAST(gx_form_y_coordinate AS VARCHAR)),     '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_form_y_coordinate,
            LIST(IFNULL(gx_gross_thickness,       '{PURR_NULL}', CAST(gx_gross_thickness AS VARCHAR)),       '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_gross_thickness,
            LIST(IFNULL(gx_internal_no,           '{PURR_NULL}', CAST(gx_internal_no AS VARCHAR)),           '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_internal_no,
            LIST(IFNULL(gx_net_thickness,         '{PURR_NULL}', CAST(gx_net_thickness AS VARCHAR)),         '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_net_thickness,
            LIST(IFNULL(gx_porosity,              '{PURR_NULL}', CAST(gx_porosity AS VARCHAR)),              '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_porosity,
            LIST(IFNULL(gx_show,                  '{PURR_NULL}', CAST(gx_show AS VARCHAR)),                  '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_show,
            LIST(IFNULL(gx_strat_column,          '{PURR_NULL}', CAST(gx_strat_column AS VARCHAR)),          '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_strat_column,
            LIST(IFNULL(gx_top_subsea,            '{PURR_NULL}', CAST(gx_top_subsea AS VARCHAR)),            '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_top_subsea,
            LIST(IFNULL(gx_true_strat_thickness,  '{PURR_NULL}', CAST(gx_true_strat_thickness AS VARCHAR)),  '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_true_strat_thickness,
            LIST(IFNULL(gx_true_vert_thickness,   '{PURR_NULL}', CAST(gx_true_vert_thickness AS VARCHAR)),   '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_true_vert_thickness,
            LIST(IFNULL(gx_user1,                 '{PURR_NULL}', CAST(gx_user1 AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_user1,
            LIST(IFNULL(gx_user2,                 '{PURR_NULL}', CAST(gx_user2 AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_user2,
            LIST(IFNULL(gx_user3,                 '{PURR_NULL}', CAST(gx_user3 AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_user3,
            LIST(IFNULL(gx_vendor_no,             '{PURR_NULL}', CAST(gx_vendor_no AS VARCHAR)),             '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_vendor_no,
            LIST(IFNULL(gx_wellbore_angle,        '{PURR_NULL}', CAST(gx_wellbore_angle AS VARCHAR)),        '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_wellbore_angle,
            LIST(IFNULL(gx_wellbore_azimuth,      '{PURR_NULL}', CAST(gx_wellbore_azimuth AS VARCHAR)),      '{PURR_DELIM}' ORDER BY form_obs_no) AS f_gx_wellbore_azimuth,
            LIST(IFNULL(percent_thickness,        '{PURR_NULL}', CAST(percent_thickness AS VARCHAR)),        '{PURR_DELIM}' ORDER BY form_obs_no) AS f_percent_thickness,
            LIST(IFNULL(pick_location,            '{PURR_NULL}', CAST(pick_location AS VARCHAR)),            '{PURR_DELIM}' ORDER BY form_obs_no) AS f_pick_location,
            LIST(IFNULL(pick_qualifier,           '{PURR_NULL}', CAST(pick_qualifier AS VARCHAR)),           '{PURR_DELIM}' ORDER BY form_obs_no) AS f_pick_qualifier,
            LIST(IFNULL(pick_quality,             '{PURR_NULL}', CAST(pick_quality AS VARCHAR)),             '{PURR_DELIM}' ORDER BY form_obs_no) AS f_pick_quality,
            LIST(IFNULL(pick_type,                '{PURR_NULL}', CAST(pick_type AS VARCHAR)),                '{PURR_DELIM}' ORDER BY form_obs_no) AS f_pick_type,
            LIST(IFNULL(public,                   '{PURR_NULL}', CAST(public AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY form_obs_no) AS f_public,
            LIST(IFNULL(remark,                   '{PURR_NULL}', CAST(remark AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY form_obs_no) AS f_remark,
            LIST(IFNULL(row_changed_date,         '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),         '{PURR_DELIM}' ORDER BY form_obs_no) AS f_row_changed_date,
            LIST(IFNULL(source,                   '{PURR_NULL}', CAST(source AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY form_obs_no) AS f_source,
            LIST(IFNULL(unc_fault_obs_no,         '{PURR_NULL}', CAST(unc_fault_obs_no AS VARCHAR)),         '{PURR_DELIM}' ORDER BY form_obs_no) AS f_unc_fault_obs_no,
            LIST(IFNULL(unc_fault_source,         '{PURR_NULL}', CAST(unc_fault_source AS VARCHAR)),         '{PURR_DELIM}' ORDER BY form_obs_no) AS f_unc_fault_source,
            LIST(IFNULL(unconformity_name,        '{PURR_NULL}', CAST(unconformity_name AS VARCHAR)),        '{PURR_DELIM}' ORDER BY form_obs_no) AS f_unconformity_name,
            LIST(IFNULL(uwi,                      '{PURR_NULL}', CAST(uwi AS VARCHAR)),                      '{PURR_DELIM}' ORDER BY form_obs_no) AS f_uwi
        FROM well_formation
        GROUP BY uwi
    )
    SELECT
        w.*,
        f.*
    FROM w
    JOIN f ON w.w_uwi = f.id_f_uwi
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        DISTINCT f.uwi AS w_uwi
    FROM well_formation f
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "f_": "well_formation",
    },
    "xforms": {
        "f_dominant_lithology": "array_of_string",
        "f_fault_name": "array_of_string",
        "f_faulted_ind": "array_of_bool",
        "f_form_depth": "array_of_float",
        "f_form_id": "array_of_string",
        "f_form_obs_no": "array_of_int",
        "f_form_tvd": "array_of_float",
        "f_gap_thickness": "array_of_float",
        "f_gx_base_subsea": "array_of_float",
        "f_gx_dip": "array_of_string",
        "f_gx_dip_azimuth": "array_of_string",
        "f_gx_eroded_ind": "array_of_bool",
        "f_gx_exten_id": "array_of_string",
        "f_gx_form_base_depth": "array_of_float",
        "f_gx_form_base_tvd": "array_of_float",
        "f_gx_form_depth_datum": "array_of_float",
        "f_gx_form_id_alias": "array_of_string",
        "f_gx_form_top_depth": "array_of_float",
        "f_gx_form_top_tvd": "array_of_float",
        "f_gx_form_x_coordinate": "array_of_float",
        "f_gx_form_y_coordinate": "array_of_float",
        "f_gx_gross_thickness": "array_of_float",
        "f_gx_internal_no": "array_of_float",
        "f_gx_net_thickness": "array_of_float",
        "f_gx_porosity": "array_of_float",
        "f_gx_show": "array_of_string",
        "f_gx_strat_column": "array_of_string",
        "f_gx_top_subsea": "array_of_float",
        "f_gx_true_strat_thickness": "array_of_float",
        "f_gx_true_vert_thickness": "array_of_float",
        "f_gx_user1": "array_of_string",
        "f_gx_user2": "array_of_string",
        "f_gx_user3": "array_of_string",
        "f_gx_vendor_no": "array_of_int",
        "f_gx_wellbore_angle": "array_of_float",
        "f_gx_wellbore_azimuth": "array_of_float",
        "f_percent_thickness": "array_of_float",
        "f_pick_location": "array_of_string",
        "f_pick_qualifier": "array_of_string",
        "f_pick_quality": "array_of_string",
        "f_pick_type": "array_of_string",
        "f_public": "array_of_string",
        "f_remark": "array_of_string",
        "f_row_changed_date": "array_of_datetime",
        "f_source": "array_of_string",
        "f_unc_fault_obs_no": "array_of_int",
        "f_unc_fault_source": "array_of_string",
        "f_unconformity_name": "array_of_string",
        "f_uwi": "array_of_string",
    },
    "chunk_size": 1000,
}
