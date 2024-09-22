# pylint: disable=line-too-long
"""GeoGraphix core"""

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
    c AS (
        SELECT
            base_depth                  AS c_base_depth,
            base_depth_ouom             AS c_base_depth_ouom,
            core_id                     AS c_core_id,
            core_type                   AS c_core_type,
            gx_primary_core_form_alias  AS c_gx_primary_core_form_alias,
            gx_qualifying_field         AS c_gx_qualifying_field,
            gx_remark                   AS c_gx_remark,
            gx_user1                    AS c_gx_user1,
            primary_core_form           AS c_primary_core_form,
            recovered_amt               AS c_recovered_amt,
            recovery_amt_ouom           AS c_recovery_amt_ouom,
            recovery_date               AS c_recovery_date,
            reported_core_number        AS c_reported_core_number,
            row_changed_date            AS c_row_changed_date,
            source                      AS c_source,
            top_depth                   AS c_top_depth,
            top_depth_ouom              AS c_top_depth_ouom,
            uwi                         AS c_uwi
        FROM well_core
    ),
    s AS (
        SELECT
            core_id                                AS id_core_id,
            source                                 AS id_source,
            uwi                                    AS id_uwi,
            MAX(row_changed_date)                  AS max_row_changed_date,
            LIST(IFNULL(analysis_obs_no,           '{PURR_NULL}',  CAST(analysis_obs_no AS VARCHAR)),           '{PURR_DELIM}' ORDER BY core_id) AS s_analysis_obs_no,
            LIST(IFNULL(core_id,                   '{PURR_NULL}',  CAST(core_id AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY core_id) AS s_core_id,
            LIST(IFNULL(gas_sat_vol,               '{PURR_NULL}',  CAST(gas_sat_vol AS VARCHAR)),               '{PURR_DELIM}' ORDER BY core_id) AS s_gas_sat_vol,
            LIST(IFNULL(grain_density,             '{PURR_NULL}',  CAST(grain_density AS VARCHAR)),             '{PURR_DELIM}' ORDER BY core_id) AS s_grain_density,
            LIST(IFNULL(grain_density_ouom,        '{PURR_NULL}',  CAST(grain_density_ouom AS VARCHAR)),        '{PURR_DELIM}' ORDER BY core_id) AS s_grain_density_ouom,
            LIST(IFNULL(gx_base_depth,             '{PURR_NULL}',  CAST(gx_base_depth AS VARCHAR)),             '{PURR_DELIM}' ORDER BY core_id) AS s_gx_base_depth,
            LIST(IFNULL(gx_bulk_density,           '{PURR_NULL}',  CAST(gx_bulk_density AS VARCHAR)),           '{PURR_DELIM}' ORDER BY core_id) AS s_gx_bulk_density,
            LIST(IFNULL(gx_formation,              '{PURR_NULL}',  CAST(gx_formation AS VARCHAR)),              '{PURR_DELIM}' ORDER BY core_id) AS s_gx_formation,
            LIST(IFNULL(gx_formation_alias,        '{PURR_NULL}',  CAST(gx_formation_alias AS VARCHAR)),        '{PURR_DELIM}' ORDER BY core_id) AS s_gx_formation_alias,
            LIST(IFNULL(gx_gamma_ray,              '{PURR_NULL}',  CAST(gx_gamma_ray AS VARCHAR)),              '{PURR_DELIM}' ORDER BY core_id) AS s_gx_gamma_ray,
            LIST(IFNULL(gx_lithology_desc,         '{PURR_NULL}',  CAST(gx_lithology_desc AS VARCHAR)),         '{PURR_DELIM}' ORDER BY core_id) AS s_gx_lithology_desc,
            LIST(IFNULL(gx_poissons_ratio,         '{PURR_NULL}',  CAST(gx_poissons_ratio AS VARCHAR)),         '{PURR_DELIM}' ORDER BY core_id) AS s_gx_poissons_ratio,
            LIST(IFNULL(gx_remark,                 '{PURR_NULL}',  CAST(gx_remark AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY core_id) AS s_gx_remark,
            LIST(IFNULL(gx_resistivity,            '{PURR_NULL}',  CAST(gx_resistivity AS VARCHAR)),            '{PURR_DELIM}' ORDER BY core_id) AS s_gx_resistivity,
            LIST(IFNULL(gx_shift_depth,            '{PURR_NULL}',  CAST(gx_shift_depth AS VARCHAR)),            '{PURR_DELIM}' ORDER BY core_id) AS s_gx_shift_depth,
            LIST(IFNULL(gx_show_type,              '{PURR_NULL}',  CAST(gx_show_type AS VARCHAR)),              '{PURR_DELIM}' ORDER BY core_id) AS s_gx_show_type,
            LIST(IFNULL(gx_toc,                    '{PURR_NULL}',  CAST(gx_toc AS VARCHAR)),                    '{PURR_DELIM}' ORDER BY core_id) AS s_gx_toc,
            LIST(IFNULL(gx_total_clay,             '{PURR_NULL}',  CAST(gx_total_clay AS VARCHAR)),             '{PURR_DELIM}' ORDER BY core_id) AS s_gx_total_clay,
            LIST(IFNULL(gx_vitrinite_reflectance,  '{PURR_NULL}',  CAST(gx_vitrinite_reflectance AS VARCHAR)),  '{PURR_DELIM}' ORDER BY core_id) AS s_gx_vitrinite_reflectance,
            LIST(IFNULL(gx_youngs_modulus,         '{PURR_NULL}',  CAST(gx_youngs_modulus AS VARCHAR)),         '{PURR_DELIM}' ORDER BY core_id) AS s_gx_youngs_modulus,
            LIST(IFNULL(k90,                       '{PURR_NULL}',  CAST(k90 AS VARCHAR)),                       '{PURR_DELIM}' ORDER BY core_id) AS s_k90,
            LIST(IFNULL(k90_ouom,                  '{PURR_NULL}',  CAST(k90_ouom AS VARCHAR)),                  '{PURR_DELIM}' ORDER BY core_id) AS s_k90_ouom,
            LIST(IFNULL(kmax,                      '{PURR_NULL}',  CAST(kmax AS VARCHAR)),                      '{PURR_DELIM}' ORDER BY core_id) AS s_kmax,
            LIST(IFNULL(kmax_ouom,                 '{PURR_NULL}',  CAST(kmax_ouom AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY core_id) AS s_kmax_ouom,
            LIST(IFNULL(kvert,                     '{PURR_NULL}',  CAST(kvert AS VARCHAR)),                     '{PURR_DELIM}' ORDER BY core_id) AS s_kvert,
            LIST(IFNULL(kvert_ouom,                '{PURR_NULL}',  CAST(kvert_ouom AS VARCHAR)),                '{PURR_DELIM}' ORDER BY core_id) AS s_kvert_ouom,
            LIST(IFNULL(oil_sat,                   '{PURR_NULL}',  CAST(oil_sat AS VARCHAR)),                   '{PURR_DELIM}' ORDER BY core_id) AS s_oil_sat,
            LIST(IFNULL(pore_volume_oil_sat,       '{PURR_NULL}',  CAST(pore_volume_oil_sat AS VARCHAR)),       '{PURR_DELIM}' ORDER BY core_id) AS s_pore_volume_oil_sat,
            LIST(IFNULL(pore_volume_water_sat,     '{PURR_NULL}',  CAST(pore_volume_water_sat AS VARCHAR)),     '{PURR_DELIM}' ORDER BY core_id) AS s_pore_volume_water_sat,
            LIST(IFNULL(porosity,                  '{PURR_NULL}',  CAST(porosity AS VARCHAR)),                  '{PURR_DELIM}' ORDER BY core_id) AS s_porosity,
            LIST(IFNULL(row_changed_date,          '{PURR_NULL}',  CAST(row_changed_date AS VARCHAR)),          '{PURR_DELIM}' ORDER BY core_id) AS s_row_changed_date,
            LIST(IFNULL(sample_id,                 '{PURR_NULL}',  CAST(sample_id AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY core_id) AS s_sample_id,
            LIST(IFNULL(sample_number,             '{PURR_NULL}',  CAST(sample_number AS VARCHAR)),             '{PURR_DELIM}' ORDER BY core_id) AS s_sample_number,
            LIST(IFNULL(source,                    '{PURR_NULL}',  CAST(source AS VARCHAR)),                    '{PURR_DELIM}' ORDER BY core_id) AS s_source,
            LIST(IFNULL(top_depth,                 '{PURR_NULL}',  CAST(top_depth AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY core_id) AS s_top_depth,
            LIST(IFNULL(top_depth_ouom,            '{PURR_NULL}',  CAST(top_depth_ouom AS VARCHAR)),            '{PURR_DELIM}' ORDER BY core_id) AS s_top_depth_ouom,
            LIST(IFNULL(uwi,                       '{PURR_NULL}',  CAST(uwi AS VARCHAR)),                       '{PURR_DELIM}' ORDER BY core_id) AS s_uwi,
            LIST(IFNULL(water_sat,                 '{PURR_NULL}',  CAST(water_sat AS VARCHAR)),                 '{PURR_DELIM}' ORDER BY core_id) AS s_water_sat
        FROM well_core_sample_anal
        GROUP BY id_uwi, id_source, id_core_id
    )
    SELECT
        w.*,
        c.*,
        s.*
    FROM w
    JOIN c ON c.c_uwi = w.w_uwi
    LEFT OUTER JOIN s
        ON c.c_uwi = s.id_uwi
        AND c.c_source = s.id_source
        AND c.c_core_id = s.id_core_id
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        DISTINCT c.uwi AS w_uwi
    FROM well_core c
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "c_": "well_core",
        "s_": "well_core_sample_anal",
    },
    "xforms": {
        "s_analysis_obs_no": "array_of_int",
        "s_core_id": "array_of_string",
        "s_gas_sat_vol": "array_of_float",
        "s_grain_density": "array_of_float",
        "s_grain_density_ouom": "array_of_string",
        "s_gx_base_depth": "array_of_float",
        "s_gx_bulk_density": "array_of_float",
        "s_gx_formation": "array_of_string",
        "s_gx_formation_alias": "array_of_string",
        "s_gx_gamma_ray": "array_of_float",
        "s_gx_lithology_desc": "array_of_string",
        "s_gx_poissons_ratio": "array_of_float",
        "s_gx_remark": "array_of_string",
        "s_gx_resistivity": "array_of_float",
        "s_gx_shift_depth": "array_of_float",
        "s_gx_show_type": "array_of_string",
        "s_gx_toc": "array_of_float",
        "s_gx_total_clay": "array_of_float",
        "s_gx_user1": "array_of_float",
        "s_gx_user2": "array_of_float",
        "s_gx_user3": "array_of_float",
        "s_gx_user4": "array_of_float",
        "s_gx_user5": "array_of_float",
        "s_gx_user6": "array_of_float",
        "s_gx_user7": "array_of_float",
        "s_gx_user8": "array_of_float",
        "s_gx_user9": "array_of_float",
        "s_gx_user10": "array_of_float",
        "s_gx_user11": "array_of_float",
        "s_gx_user12": "array_of_float",
        "s_gx_user13": "array_of_float",
        "s_gx_user14": "array_of_float",
        "s_gx_user15": "array_of_float",
        "s_gx_user16": "array_of_float",
        "s_gx_user17": "array_of_float",
        "s_gx_user18": "array_of_float",
        "s_gx_user19": "array_of_float",
        "s_gx_user20": "array_of_float",
        "s_gx_vitrinite_reflectance": "array_of_float",
        "s_gx_youngs_modulus": "array_of_float",
        "s_k90": "array_of_float",
        "s_k90_ouom": "array_of_string",
        "s_kmax": "array_of_float",
        "s_kmax_ouom": "array_of_string",
        "s_kvert": "array_of_float",
        "s_kvert_ouom": "array_of_string",
        "s_oil_sat": "array_of_float",
        "s_pore_volume_oil_sat": "array_of_float",
        "s_pore_volume_water_sat": "array_of_float",
        "s_porosity": "array_of_float",
        "s_row_changed_date": "array_of_datetime",
        "s_sample_id": "array_of_string",
        "s_sample_number": "array_of_int",
        "s_source": "array_of_string",
        "s_top_depth": "array_of_float",
        "s_top_depth_ouom": "array_of_string",
        "s_uwi": "array_of_string",
        "s_water_sat": "array_of_float",
    },
    "chunk_size": 1000,
}
