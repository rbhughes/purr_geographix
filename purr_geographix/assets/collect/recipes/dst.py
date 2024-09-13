"""GeoGraphix dst"""

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
    t AS (
        SELECT
            uwi                        AS id_t_uwi,
            source                     AS id_t_source,
            test_type                  AS id_t_test_type,
            run_number                 AS id_t_run_number,
            MAX(row_changed_date)      AS max_row_changed_date,
            LIST(IFNULL(base_depth,                 '{PURR_NULL}', CAST(base_depth AS VARCHAR)),               '{PURR_DELIM}' ORDER BY test_number)  AS t_base_depth,
            LIST(IFNULL(base_form,                  '{PURR_NULL}', CAST(base_form AS VARCHAR)),                '{PURR_DELIM}' ORDER BY test_number)  AS t_base_form,
            LIST(IFNULL(bottom_choke_desc,          '{PURR_NULL}', CAST(bottom_choke_desc AS VARCHAR)),        '{PURR_DELIM}' ORDER BY test_number)  AS t_bottom_choke_desc,
            LIST(IFNULL(casing_press,               '{PURR_NULL}', CAST(casing_press AS VARCHAR)),             '{PURR_DELIM}' ORDER BY test_number)  AS t_casing_press,
            LIST(IFNULL(choke_size_desc,            '{PURR_NULL}', CAST(choke_size_desc AS VARCHAR)),          '{PURR_DELIM}' ORDER BY test_number)  AS t_choke_size_desc,
            LIST(IFNULL(flow_press,                 '{PURR_NULL}', CAST(flow_press AS VARCHAR)),               '{PURR_DELIM}' ORDER BY test_number)  AS t_flow_press,
            LIST(IFNULL(flow_temp,                  '{PURR_NULL}', CAST(flow_temp AS VARCHAR)),                '{PURR_DELIM}' ORDER BY test_number)  AS t_flow_temp,
            LIST(IFNULL(gas_flow_amt,               '{PURR_NULL}', CAST(gas_flow_amt AS VARCHAR)),             '{PURR_DELIM}' ORDER BY test_number)  AS t_gas_flow_amt,
            LIST(IFNULL(gas_flow_amt_uom,           '{PURR_NULL}', CAST(gas_flow_amt_uom AS VARCHAR)),         '{PURR_DELIM}' ORDER BY test_number)  AS t_gas_flow_amt_uom,
            LIST(IFNULL(gor,                        '{PURR_NULL}', CAST(gor AS VARCHAR)),                      '{PURR_DELIM}' ORDER BY test_number)  AS t_gor,
            LIST(IFNULL(gx_base_form_alias,         '{PURR_NULL}', CAST(gx_base_form_alias AS VARCHAR)),       '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_base_form_alias,
            LIST(IFNULL(gx_bhp_z,                   '{PURR_NULL}', CAST(gx_bhp_z AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_bhp_z,
            LIST(IFNULL(gx_co2_pct,                 '{PURR_NULL}', CAST(gx_co2_pct AS VARCHAR)),               '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_co2_pct,
            LIST(IFNULL(gx_conversion_factor,       '{PURR_NULL}', CAST(gx_conversion_factor AS VARCHAR)),     '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_conversion_factor,
            LIST(IFNULL(gx_cushion_type,            '{PURR_NULL}', CAST(gx_cushion_type AS VARCHAR)),          '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_cushion_type,
            LIST(IFNULL(gx_cushion_vol,             '{PURR_NULL}', CAST(gx_cushion_vol AS VARCHAR)),           '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_cushion_vol,
            LIST(IFNULL(gx_cushion_vol_ouom,        '{PURR_NULL}', CAST(gx_cushion_vol_ouom AS VARCHAR)),      '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_cushion_vol_ouom,
            LIST(IFNULL(gx_ft_flow_amt,             '{PURR_NULL}', CAST(gx_ft_flow_amt AS VARCHAR)),           '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_ft_flow_amt,
            LIST(IFNULL(gx_ft_flow_amt_uom,         '{PURR_NULL}', CAST(gx_ft_flow_amt_uom AS VARCHAR)),       '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_ft_flow_amt_uom,
            LIST(IFNULL(gx_gas_cum,                 '{PURR_NULL}', CAST(gx_gas_cum AS VARCHAR)),               '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_gas_cum,
            LIST(IFNULL(gx_max_ft_flow_rate,        '{PURR_NULL}', CAST(gx_max_ft_flow_rate AS VARCHAR)),      '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate,
            LIST(IFNULL(gx_max_ft_flow_rate_uom,    '{PURR_NULL}', CAST(gx_max_ft_flow_rate_uom AS VARCHAR)),  '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate_uom,
            LIST(IFNULL(gx_recorder_depth,          '{PURR_NULL}', CAST(gx_recorder_depth AS VARCHAR)),        '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_recorder_depth,
            LIST(IFNULL(gx_top_form_alias,          '{PURR_NULL}', CAST(gx_top_form_alias AS VARCHAR)),        '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_top_form_alias,
            LIST(IFNULL(gx_tracer,                  '{PURR_NULL}', CAST(gx_tracer AS VARCHAR)),                '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_tracer,
            LIST(IFNULL(gx_z_factor,                '{PURR_NULL}', CAST(gx_z_factor AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS t_gx_z_factor,
            LIST(IFNULL(h2s_pct,                    '{PURR_NULL}', CAST(h2s_pct AS VARCHAR)),                  '{PURR_DELIM}' ORDER BY test_number)  AS t_h2s_pct,
            LIST(IFNULL(max_gas_flow_rate,          '{PURR_NULL}', CAST(max_gas_flow_rate AS VARCHAR)),        '{PURR_DELIM}' ORDER BY test_number)  AS t_max_gas_flow_rate,
            LIST(IFNULL(max_hydrostatic_press,      '{PURR_NULL}', CAST(max_hydrostatic_press AS VARCHAR)),    '{PURR_DELIM}' ORDER BY test_number)  AS t_max_hydrostatic_press,
            LIST(IFNULL(max_oil_flow_rate,          '{PURR_NULL}', CAST(max_oil_flow_rate AS VARCHAR)),        '{PURR_DELIM}' ORDER BY test_number)  AS t_max_oil_flow_rate,
            LIST(IFNULL(max_water_flow_rate,        '{PURR_NULL}', CAST(max_water_flow_rate AS VARCHAR)),      '{PURR_DELIM}' ORDER BY test_number)  AS t_max_water_flow_rate,
            LIST(IFNULL(oil_flow_amt,               '{PURR_NULL}', CAST(oil_flow_amt AS VARCHAR)),             '{PURR_DELIM}' ORDER BY test_number)  AS t_oil_flow_amt,
            LIST(IFNULL(oil_flow_amt_uom,           '{PURR_NULL}', CAST(oil_flow_amt_uom AS VARCHAR)),         '{PURR_DELIM}' ORDER BY test_number)  AS t_oil_flow_amt_uom,
            LIST(IFNULL(oil_gravity,                '{PURR_NULL}', CAST(oil_gravity AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS t_oil_gravity,
            LIST(IFNULL(primary_fluid_recovered,    '{PURR_NULL}', CAST(primary_fluid_recovered AS VARCHAR)),  '{PURR_DELIM}' ORDER BY test_number)  AS t_primary_fluid_recovered,
            LIST(IFNULL(production_method,          '{PURR_NULL}', CAST(production_method AS VARCHAR)),        '{PURR_DELIM}' ORDER BY test_number)  AS t_production_method,
            LIST(IFNULL(remark,                     '{PURR_NULL}', CAST(remark AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY test_number)  AS t_remark,
            LIST(IFNULL(report_temp,                '{PURR_NULL}', CAST(report_temp AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS t_report_temp,
            LIST(IFNULL(row_changed_date,           '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),         '{PURR_DELIM}' ORDER BY test_number)  AS t_row_changed_date,
            LIST(IFNULL(shut_off_type,              '{PURR_NULL}', CAST(shut_off_type AS VARCHAR)),            '{PURR_DELIM}' ORDER BY test_number)  AS t_shut_off_type,
            LIST(IFNULL(test_date,                  '{PURR_NULL}', CAST(test_date AS VARCHAR)),                '{PURR_DELIM}' ORDER BY test_number)  AS t_test_date,
            LIST(IFNULL(test_duration,              '{PURR_NULL}', CAST(test_duration AS VARCHAR)),            '{PURR_DELIM}' ORDER BY test_number)  AS t_test_duration,
            LIST(IFNULL(test_number,                '{PURR_NULL}', CAST(test_number AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS t_test_number,
            LIST(IFNULL(test_subtype,               '{PURR_NULL}', CAST(test_subtype AS VARCHAR)),             '{PURR_DELIM}' ORDER BY test_number)  AS t_test_subtype,
            LIST(IFNULL(top_choke_desc,             '{PURR_NULL}', CAST(top_choke_desc AS VARCHAR)),           '{PURR_DELIM}' ORDER BY test_number)  AS t_top_choke_desc,
            LIST(IFNULL(top_depth,                  '{PURR_NULL}', CAST(top_depth AS VARCHAR)),                '{PURR_DELIM}' ORDER BY test_number)  AS t_top_depth,
            LIST(IFNULL(top_form,                   '{PURR_NULL}', CAST(top_form AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY test_number)  AS t_top_form,
            LIST(IFNULL(water_flow_amt,             '{PURR_NULL}', CAST(water_flow_amt AS VARCHAR)),           '{PURR_DELIM}' ORDER BY test_number)  AS t_water_flow_amt,
            LIST(IFNULL(water_flow_amt_uom,         '{PURR_NULL}', CAST(water_flow_amt_uom AS VARCHAR)),       '{PURR_DELIM}' ORDER BY test_number)  AS t_water_flow_amt_uom
        FROM well_test
        WHERE test_type = 'DST'
        GROUP BY uwi, source, test_type, run_number
    ), 
    s AS (
        SELECT
            uwi                        AS id_s_uwi,
            source                     AS id_s_source,
            test_type                  AS id_s_test_type,
            run_number                 AS id_s_run_number,
            LIST(IFNULL(end_press,                  '{PURR_NULL}', CAST(end_press AS VARCHAR)),                '{PURR_DELIM}' ORDER BY test_number)  AS s_end_press,
            LIST(IFNULL(gx_duration,                '{PURR_NULL}', CAST(gx_duration AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS s_gx_duration,
            LIST(IFNULL(period_type,                '{PURR_NULL}', CAST(period_type AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS s_period_type,
            LIST(IFNULL(row_changed_date,           '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),         '{PURR_DELIM}' ORDER BY test_number)  AS s_row_changed_date,
            LIST(IFNULL(start_press,                '{PURR_NULL}', CAST(start_press AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS s_start_press,
            LIST(IFNULL(test_number,                '{PURR_NULL}', CAST(test_number AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS s_test_number
        FROM well_test_pressure
        GROUP BY uwi, source, test_type, run_number
    ),
    p AS (
        SELECT
            uwi                        AS id_p_uwi,
            source                     AS id_p_source,
            test_type                  AS id_p_test_type,
            run_number                 AS id_p_run_number,
            LIST(IFNULL(recovery_amt,               '{PURR_NULL}', CAST(recovery_amt AS VARCHAR)),             '{PURR_DELIM}' ORDER BY test_number)  AS p_recovery_amt,
            LIST(IFNULL(recovery_amt_uom,           '{PURR_NULL}', CAST(recovery_amt_uom AS VARCHAR)),         '{PURR_DELIM}' ORDER BY test_number)  AS p_recovery_amt_uom,
            LIST(IFNULL(recovery_method,            '{PURR_NULL}', CAST(recovery_method AS VARCHAR)),          '{PURR_DELIM}' ORDER BY test_number)  AS p_recovery_method,
            LIST(IFNULL(recovery_obs_no,            '{PURR_NULL}', CAST(recovery_obs_no AS VARCHAR)),          '{PURR_DELIM}' ORDER BY test_number)  AS p_recovery_obs_no,
            LIST(IFNULL(recovery_type,              '{PURR_NULL}', CAST(recovery_type AS VARCHAR)),            '{PURR_DELIM}' ORDER BY test_number)  AS p_recovery_type,
            LIST(IFNULL(remark,                     '{PURR_NULL}', CAST(remark AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY test_number)  AS p_remark,
            LIST(IFNULL(row_changed_date,           '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),         '{PURR_DELIM}' ORDER BY test_number)  AS p_row_changed_date,
            LIST(IFNULL(test_number,                '{PURR_NULL}', CAST(test_number AS VARCHAR)),              '{PURR_DELIM}' ORDER BY test_number)  AS p_test_number
        FROM well_test_recovery
        WHERE recovery_method = 'PIPE'
        GROUP BY uwi, source, test_type, run_number
    ),
    f AS (
        SELECT
            uwi                        AS id_f_uwi,
            source                     AS id_f_source,
            test_type                  AS id_f_test_type,
            run_number                 AS id_f_run_number,
            LIST(IFNULL(fluid_type,                '{PURR_NULL}', CAST(fluid_type AS VARCHAR)),                '{PURR_DELIM}' ORDER BY test_number)  AS f_fluid_type,
            LIST(IFNULL(max_fluid_rate,            '{PURR_NULL}', CAST(max_fluid_rate AS VARCHAR)),            '{PURR_DELIM}' ORDER BY test_number)  AS f_max_fluid_rate,
            LIST(IFNULL(max_fluid_rate_uom,        '{PURR_NULL}', CAST(max_fluid_rate_uom AS VARCHAR)),        '{PURR_DELIM}' ORDER BY test_number)  AS f_max_fluid_rate_uom,
            LIST(IFNULL(row_changed_date,          '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),          '{PURR_DELIM}' ORDER BY test_number)  AS f_row_changed_date,
            LIST(IFNULL(test_number,               '{PURR_NULL}', CAST(test_number AS VARCHAR)),               '{PURR_DELIM}' ORDER BY test_number)  AS f_test_number,
            LIST(IFNULL(tts_elasped_time,          '{PURR_NULL}', CAST(tts_elasped_time AS VARCHAR)),          '{PURR_DELIM}' ORDER BY test_number)  AS f_tts_elasped_time  --sic
        FROM well_test_flow
        GROUP BY uwi, source, test_type, run_number
    )
    SELECT
        w.*,
        t.*,
        s.*,
        p.*,
        f.*
    FROM w
    JOIN t ON w.w_uwi = t.id_t_uwi
    LEFT OUTER JOIN s 
        ON s.id_s_uwi = t.id_t_uwi
        AND s.id_s_source = t.id_t_source
        AND s.id_s_test_type = t.id_t_test_type
        AND s.id_s_run_number = t.id_t_run_number
    LEFT OUTER JOIN p
        ON p.id_p_uwi = t.id_t_uwi
        AND p.id_p_source = t.id_t_source
        AND p.id_p_test_type = t.id_t_test_type
        AND p.id_p_run_number = t.id_t_run_number
    LEFT OUTER JOIN f
        ON f.id_f_uwi = t.id_t_uwi
        AND f.id_f_source = t.id_t_source
        AND f.id_f_test_type = t.id_t_test_type
        AND f.id_f_run_number = t.id_t_run_number
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        DISTINCT w.uwi AS w_uwi
    FROM well w
    JOIN well_test t
        ON w.uwi = t.uwi AND t.test_type = 'DST'
    {PURR_WHERE}
    """
recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "t_": "well_test",
        "s_": "well_test_pressure",
        "p_": "well_test_recovery",
        "f_": "well_test_flow",
    },
    "xforms": {
        "t_base_depth": "array_of_float",
        "t_base_form": "array_of_string",
        "t_bottom_choke_desc": "array_of_string",
        "t_casing_press": "array_of_float",
        "t_choke_size_desc": "array_of_string",
        "t_flow_press": "array_of_float",
        "t_flow_temp": "array_of_float",
        "t_gas_flow_amt": "array_of_float",
        "t_gas_flow_amt_uom": "array_of_string",
        "t_gor": "array_of_float",
        "t_gx_base_form_alias": "array_of_string",
        "t_gx_bhp_z": "array_of_float",
        "t_gx_co2_pct": "array_of_float",
        "t_gx_conversion_factor": "array_of_float",
        "t_gx_cushion_type": "array_of_string",
        "t_gx_cushion_vol": "array_of_float",
        "t_gx_cushion_vol_ouom": "array_of_string",
        "t_gx_ft_flow_amt": "array_of_float",
        "t_gx_ft_flow_amt_uom": "array_of_string",
        "t_gx_gas_cum": "array_of_float",
        "t_gx_max_ft_flow_rate": "array_of_float",
        "t_gx_max_ft_flow_rate_uom": "array_of_string",
        "t_gx_recorder_depth": "array_of_float",
        "t_gx_top_form_alias": "array_of_string",
        "t_gx_tracer": "array_of_string",
        "t_gx_z_factor": "array_of_float",
        "t_h2s_pct": "array_of_float",
        "t_max_gas_flow_rate": "array_of_string",
        "t_max_hydrostatic_press": "array_of_float",
        "t_max_oil_flow_rate": "array_of_string",
        "t_max_water_flow_rate": "array_of_string",
        "t_oil_flow_amt": "array_of_float",
        "t_oil_flow_amt_uom": "array_of_string",
        "t_oil_gravity": "array_of_float",
        "t_primary_fluid_recovered": "array_of_string",
        "t_production_method": "array_of_string",
        "t_remark": "array_of_string",
        "t_report_temp": "array_of_float",
        "t_row_changed_date": "array_of_datetime",
        "t_shut_off_type": "array_of_string",
        "t_test_date": "array_of_datetime",
        "t_test_duration": "array_of_string",
        "t_test_number": "array_of_string",
        "t_test_subtype": "array_of_string",
        "t_top_choke_desc": "array_of_string",
        "t_top_depth": "array_of_float",
        "t_top_form": "array_of_string",
        "t_water_flow_amt": "array_of_float",
        "t_water_flow_amt_uom": "array_of_string",
        "s_end_press": "array_of_float",
        "s_gx_duration": "array_of_string",
        "s_period_type": "array_of_string",
        "s_row_changed_date": "array_of_datetime",
        "s_start_press": "array_of_float",
        "s_test_number": "array_of_int",
        "p_recovery_amt": "array_of_float",
        "p_recovery_amt_uom": "array_of_string",
        "p_recovery_method": "array_of_string",
        "p_recovery_obs_no": "array_of_int",
        "p_recovery_type": "array_of_string",
        "p_remark": "array_of_string",
        "p_row_changed_date": "array_of_datetime",
        "p_test_number": "array_of_string",
        "f_fluid_type": "array_of_string",
        "f_max_fluid_rate": "array_of_float",
        "f_max_fluid_rate_uom": "array_of_string",
        "f_row_changed_date": "array_of_datetime",
        "f_test_number": "array_of_int",
        "p_tts_elapsed_time": "array_of_string",
    },
    "chunk_size": 100,
}
