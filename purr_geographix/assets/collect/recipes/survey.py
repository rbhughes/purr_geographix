# pylint: disable=line-too-long
"""GeoGraphix survey"""

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
    d AS (
        SELECT 
            uwi                                AS id_d_uwi,
            survey_id                          AS id_d_survey_id,
            source                             AS id_d_source,
            'actual'                           AS id_d_kind,
            MAX(row_changed_date)              AS max_row_changed_date,
            LIST(IFNULL(azimuth,               '{PURR_NULL}', CAST(azimuth AS VARCHAR)),               '{PURR_DELIM}' ORDER BY station_md)  AS d_azimuth,
            LIST(IFNULL(azimuth_ouom,          '{PURR_NULL}', azimuth_ouom),                           '{PURR_DELIM}' ORDER BY station_md)  AS d_azimuth_ouom,
            LIST(IFNULL(ew_direction,          '{PURR_NULL}', ew_direction),                           '{PURR_DELIM}' ORDER BY station_md)  AS d_ew_direction,
            LIST(IFNULL(gx_closure,            '{PURR_NULL}', gx_closure),                             '{PURR_DELIM}' ORDER BY station_md)  AS d_gx_closure,
            LIST(IFNULL(gx_station_latitude,   '{PURR_NULL}', CAST(gx_station_latitude AS VARCHAR)),   '{PURR_DELIM}' ORDER BY station_md)  AS d_gx_station_latitude,
            LIST(IFNULL(gx_station_longitude,  '{PURR_NULL}', CAST(gx_station_longitude AS VARCHAR)),  '{PURR_DELIM}' ORDER BY station_md)  AS d_gx_station_longitude,
            LIST(IFNULL(inclination,           '{PURR_NULL}', CAST(inclination AS VARCHAR)),           '{PURR_DELIM}' ORDER BY station_md)  AS d_inclination,
            LIST(IFNULL(inclination_ouom,      '{PURR_NULL}', inclination_ouom),                       '{PURR_DELIM}' ORDER BY station_md)  AS d_inclination_ouom,
            LIST(IFNULL(ns_direction,          '{PURR_NULL}', ns_direction),                           '{PURR_DELIM}' ORDER BY station_md)  AS d_ns_direction,
            LIST(IFNULL(row_changed_date,      '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),      '{PURR_DELIM}' ORDER BY station_md)  AS d_row_changed_date,
            LIST(IFNULL(source,                '{PURR_NULL}', source),                                 '{PURR_DELIM}' ORDER BY station_md)  AS d_source,
            LIST(IFNULL(station_id,            '{PURR_NULL}', station_id),                             '{PURR_DELIM}' ORDER BY station_md)  AS d_station_id,
            LIST(IFNULL(station_md,            '{PURR_NULL}', CAST(station_md AS VARCHAR)),            '{PURR_DELIM}' ORDER BY station_md)  AS d_station_md,
            LIST(IFNULL(station_md_ouom,       '{PURR_NULL}', station_md_ouom),                        '{PURR_DELIM}' ORDER BY station_md)  AS d_station_md_ouom,
            LIST(IFNULL(station_tvd,           '{PURR_NULL}', CAST(station_tvd AS VARCHAR)),           '{PURR_DELIM}' ORDER BY station_md)  AS d_station_tvd,
            LIST(IFNULL(station_tvd_ouom,      '{PURR_NULL}', station_tvd_ouom),                       '{PURR_DELIM}' ORDER BY station_md)  AS d_station_tvd_ouom,
            LIST(IFNULL(survey_id,             '{PURR_NULL}', survey_id),                              '{PURR_DELIM}' ORDER BY station_md)  AS d_survey_id,
            LIST(IFNULL(uwi,                   '{PURR_NULL}', uwi),                                    '{PURR_DELIM}' ORDER BY station_md)  AS d_uwi,
            LIST(IFNULL(x_offset,              '{PURR_NULL}', CAST(x_offset AS VARCHAR)),              '{PURR_DELIM}' ORDER BY station_md)  AS d_x_offset,
            LIST(IFNULL(x_offset_ouom,         '{PURR_NULL}', x_offset_ouom),                          '{PURR_DELIM}' ORDER BY station_md)  AS d_x_offset_ouom,
            LIST(IFNULL(y_offset,              '{PURR_NULL}', CAST(y_offset AS VARCHAR)),              '{PURR_DELIM}' ORDER BY station_md)  AS d_y_offset,
            LIST(IFNULL(y_offset_ouom,         '{PURR_NULL}', y_offset_ouom),                          '{PURR_DELIM}' ORDER BY station_md)  AS d_y_offset_ouom
        FROM well_dir_srvy_station
        GROUP BY uwi, survey_id, source
        UNION
        SELECT 
            uwi                                AS id_d_uwi,
            survey_id                          AS id_d_survey_id,
            source                             AS id_d_source,
            'proposed'                         AS id_d_kind,
            MAX(row_changed_date)              AS max_row_changed_date,
            LIST(IFNULL(azimuth,               '{PURR_NULL}', CAST(azimuth AS VARCHAR)),               '{PURR_DELIM}' ORDER BY station_md)  AS d_azimuth,
            LIST(IFNULL(azimuth_ouom,          '{PURR_NULL}', azimuth_ouom),                           '{PURR_DELIM}' ORDER BY station_md)  AS d_azimuth_ouom,
            LIST(IFNULL(ew_direction,          '{PURR_NULL}', ew_direction),                           '{PURR_DELIM}' ORDER BY station_md)  AS d_ew_direction,
            LIST(IFNULL(gx_closure,            '{PURR_NULL}', gx_closure),                             '{PURR_DELIM}' ORDER BY station_md)  AS d_gx_closure,
            LIST(IFNULL(gx_station_latitude,   '{PURR_NULL}', CAST(gx_station_latitude AS VARCHAR)),   '{PURR_DELIM}' ORDER BY station_md)  AS d_gx_station_latitude,
            LIST(IFNULL(gx_station_longitude,  '{PURR_NULL}', CAST(gx_station_longitude AS VARCHAR)),  '{PURR_DELIM}' ORDER BY station_md)  AS d_gx_station_longitude,
            LIST(IFNULL(inclination,           '{PURR_NULL}', CAST(inclination AS VARCHAR)),           '{PURR_DELIM}' ORDER BY station_md)  AS d_inclination,
            LIST(IFNULL(inclination_ouom,      '{PURR_NULL}', inclination_ouom),                       '{PURR_DELIM}' ORDER BY station_md)  AS d_inclination_ouom,
            LIST(IFNULL(ns_direction,          '{PURR_NULL}', ns_direction),                           '{PURR_DELIM}' ORDER BY station_md)  AS d_ns_direction,
            LIST(IFNULL(row_changed_date,      '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),      '{PURR_DELIM}' ORDER BY station_md)  AS d_row_changed_date,
            LIST(IFNULL(source,                '{PURR_NULL}', source),                                 '{PURR_DELIM}' ORDER BY station_md)  AS d_source,
            LIST(IFNULL(station_id,            '{PURR_NULL}', station_id),                             '{PURR_DELIM}' ORDER BY station_md)  AS d_station_id,
            LIST(IFNULL(station_md,            '{PURR_NULL}', CAST(station_md AS VARCHAR)),            '{PURR_DELIM}' ORDER BY station_md)  AS d_station_md,
            LIST(IFNULL(station_md_ouom,       '{PURR_NULL}', station_md_ouom),                        '{PURR_DELIM}' ORDER BY station_md)  AS d_station_md_ouom,
            LIST(IFNULL(station_tvd,           '{PURR_NULL}', CAST(station_tvd AS VARCHAR)),           '{PURR_DELIM}' ORDER BY station_md)  AS d_station_tvd,
            LIST(IFNULL(station_tvd_ouom,      '{PURR_NULL}', station_tvd_ouom),                       '{PURR_DELIM}' ORDER BY station_md)  AS d_station_tvd_ouom,
            LIST(IFNULL(survey_id,             '{PURR_NULL}', survey_id),                              '{PURR_DELIM}' ORDER BY station_md)  AS d_survey_id,
            LIST(IFNULL(uwi,                   '{PURR_NULL}', uwi),                                    '{PURR_DELIM}' ORDER BY station_md)  AS d_uwi,
            LIST(IFNULL(x_offset,              '{PURR_NULL}', CAST(x_offset AS VARCHAR)),              '{PURR_DELIM}' ORDER BY station_md)  AS d_x_offset,
            LIST(IFNULL(x_offset_ouom,         '{PURR_NULL}', x_offset_ouom),                          '{PURR_DELIM}' ORDER BY station_md)  AS d_x_offset_ouom,
            LIST(IFNULL(y_offset,              '{PURR_NULL}', CAST(y_offset AS VARCHAR)),              '{PURR_DELIM}' ORDER BY station_md)  AS d_y_offset,
            LIST(IFNULL(y_offset_ouom,         '{PURR_NULL}', y_offset_ouom),                          '{PURR_DELIM}' ORDER BY station_md)  AS d_y_offset_ouom
        FROM well_dir_proposed_srvy_station
        GROUP BY uwi, survey_id, source
    ),
    s AS (
        SELECT
            'actual'                   AS s_kind,
            s.azimuth_north_type       AS s_azimuth_north_type,
            s.base_depth               AS s_base_depth,
            s.base_depth_ouom          AS s_base_depth_ouom,
            s.calculation_required     AS s_calculation_required,
            s.compute_method           AS s_compute_method,
            s.ew_magnetic_declination  AS s_ew_magnetic_declination,
            s.grid_system_id           AS s_grid_system_id,
            null                       AS s_gx_active,
            s.gx_base_e_w_offset       AS s_gx_base_e_w_offset,
            s.gx_base_latitude         AS s_gx_base_latitude,
            s.gx_base_location_string  AS s_gx_base_location_string,
            s.gx_base_longitude        AS s_gx_base_longitude,
            s.gx_base_n_s_offset       AS s_gx_base_n_s_offset,
            s.gx_base_tvd              AS s_gx_base_tvd,
            s.gx_closure               AS s_gx_closure,
            s.gx_footage               AS s_gx_footage,
            s.gx_kop_e_w_offset        AS s_gx_kop_e_w_offset,
            s.gx_kop_latitude          AS s_gx_kop_latitude,
            s.gx_kop_longitude         AS s_gx_kop_longitude,
            s.gx_kop_md                AS s_gx_kop_md,
            s.gx_kop_n_s_offset        AS s_gx_kop_n_s_offset,
            s.gx_kop_tvd               AS s_gx_kop_tvd,
            null                       AS s_gx_lp_e_w_offset,
            null                       AS s_gx_lp_n_s_offset,
            null                       AS s_gx_lp_tvd,
            --null                       AS s_gx_proposed_well_blob,
            null                       AS s_gx_scenario_name,
            s.magnetic_declination     AS s_magnetic_declination,
            s.north_reference          AS s_north_reference,
            s.offset_north_type        AS s_offset_north_type,
            s.record_mode              AS s_record_mode,
            s.remark                   AS s_remark,
            s.row_changed_date         AS s_row_changed_date,
            s.source                   AS s_source,
            s.source_document          AS s_source_document,
            s.survey_company           AS s_survey_company,
            s.survey_date              AS s_survey_date,
            s.survey_id                AS s_survey_id,
            s.survey_quality           AS s_survey_quality,
            s.survey_type              AS s_survey_type,
            s.top_depth                AS s_top_depth,
            s.top_depth_ouom           AS s_top_depth_ouom,
            s.uwi                      AS s_uwi
        FROM well_dir_srvy s
        WHERE s.uwi IN (SELECT DISTINCT(uwi) FROM well_dir_srvy_station)
        UNION
        SELECT
            'proposed'                 AS s_kind,
            s.azimuth_north_type       AS s_azimuth_north_type,
            s.base_depth               AS s_base_depth,
            s.base_depth_ouom          AS s_base_depth_ouom,
            s.calculation_required     AS s_calculation_required,
            s.compute_method           AS s_compute_method,
            s.ew_magnetic_declination  AS s_ew_magnetic_declination,
            s.grid_system_id           AS s_grid_system_id,
            s.gx_active                AS s_gx_active,
            s.gx_base_e_w_offset       AS s_gx_base_e_w_offset,
            s.gx_base_latitude         AS s_gx_base_latitude,
            s.gx_base_location_string  AS s_gx_base_location_string,
            s.gx_base_longitude        AS s_gx_base_longitude,
            s.gx_base_n_s_offset       AS s_gx_base_n_s_offset,
            s.gx_base_tvd              AS s_gx_base_tvd,
            s.gx_closure               AS s_gx_closure,
            s.gx_footage               AS s_gx_footage,
            s.gx_kop_e_w_offset        AS s_gx_kop_e_w_offset,
            s.gx_kop_latitude          AS s_gx_kop_latitude,
            s.gx_kop_longitude         AS s_gx_kop_longitude,
            s.gx_kop_md                AS s_gx_kop_md,
            s.gx_kop_n_s_offset        AS s_gx_kop_n_s_offset,
            s.gx_kop_tvd               AS s_gx_kop_tvd,
            s.gx_lp_e_w_offset         AS s_gx_lp_e_w_offset,
            s.gx_lp_n_s_offset         AS s_gx_lp_n_s_offset,
            s.gx_lp_tvd                AS s_gx_lp_tvd,
            --s.gx_proposed_well_blob    AS s_gx_proposed_well_blob,
            s.gx_scenario_name         AS s_gx_scenario_name,
            s.magnetic_declination     AS s_magnetic_declination,
            s.north_reference          AS s_north_reference,
            s.offset_north_type        AS s_offset_north_type,
            s.record_mode              AS s_record_mode,
            s.remark                   AS s_remark,
            s.row_changed_date         AS s_row_changed_date,
            s.source                   AS s_source,
            s.source_document          AS s_source_document,
            s.survey_company           AS s_survey_company,
            s.survey_date              AS s_survey_date,
            s.survey_id                AS s_survey_id,
            s.survey_quality           AS s_survey_quality,
            s.survey_type              AS s_survey_type,
            s.top_depth                AS s_top_depth,
            s.top_depth_ouom           AS s_top_depth_ouom,
            s.uwi                      AS s_uwi
        FROM well_dir_proposed_srvy s
        WHERE s.uwi IN (SELECT DISTINCT(uwi) FROM well_dir_proposed_srvy_station)
    )
    SELECT
        w.*,
        s.*,
        d.*
    FROM w
    JOIN d ON w.w_uwi = d.id_d_uwi
    JOIN s
        ON s.s_source = d.id_d_source
        AND s.s_survey_id = d.id_d_survey_id
        AND s.s_uwi = d.id_d_uwi
        AND s.s_kind = d.id_d_kind
    {PURR_WHERE}
    """

# performance might be awful here?
identifier = f"""
    SELECT
        DISTINCT uwi AS w_uwi
    FROM (
        SELECT uwi FROM well_dir_srvy_station
        UNION
        SELECT uwi FROM well_dir_proposed_srvy_station
    ) x
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "d_": "well_dir_srvy_station",
        "s_": "well_dir_srvy",
    },
    "xforms": {
        "d_azimuth": "array_of_float",
        "d_azimuth_ouom": "array_of_string",
        "d_ew_direction": "array_of_string",
        "d_gx_closure": "array_of_string",
        "d_gx_station_latitude": "array_of_float",
        "d_gx_station_longitude": "array_of_float",
        "d_inclination": "array_of_float",
        "d_inclination_ouom": "array_of_string",
        "d_ns_direction": "array_of_string",
        "d_row_changed_date": "array_of_datetime",
        "d_source": "array_of_string",
        "d_station_id": "array_of_string",
        "d_station_md": "array_of_float",
        "d_station_md_ouom": "array_of_string",
        "d_station_tvd": "array_of_float",
        "d_station_tvd_ouom": "array_of_string",
        "d_survey_id": "array_of_string",
        "d_uwi": "array_of_string",
        "d_x_offset": "array_of_float",
        "d_x_offset_ouom": "array_of_string",
        "d_y_offset": "array_of_float",
        "d_y_offset_ouom": "array_of_string",
    },
    "post_process": "survey_agg",
    "chunk_size": 1000,
}
