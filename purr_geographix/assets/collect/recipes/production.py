"""GeoGraphix production"""

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
    p AS (
        SELECT
            activity_type                    AS p_activity_type,
            average_gas_volume               AS p_average_gas_volume,
            average_injection_volume         AS p_average_injection_volume,
            average_oil_volume               AS p_average_oil_volume,
            average_total_fluids_volume      AS p_average_total_fluids_volume,
            average_water_volume             AS p_average_water_volume,
            cumulative_gas_volume            AS p_cumulative_gas_volume,
            cumulative_injection_volume      AS p_cumulative_injection_volume,
            cumulative_oil_volume            AS p_cumulative_oil_volume,
            cumulative_total_fluids_volume   AS p_cumulative_total_fluids_volume,
            cumulative_water_volume          AS p_cumulative_water_volume,
            first_gas_volume                 AS p_first_gas_volume,
            first_gas_volume_date            AS p_first_gas_volume_date,
            first_injection_volume           AS p_first_injection_volume,
            first_oil_volume                 AS p_first_oil_volume,
            first_oil_volume_date            AS p_first_oil_volume_date,
            first_total_fluids_date          AS p_first_total_fluids_date,
            first_total_fluids_volume        AS p_first_total_fluids_volume,
            first_water_volume               AS p_first_water_volume,
            first_water_volume_date          AS p_first_water_volume_date,
            gas_eur                          AS p_gas_eur,
            gas_reserves                     AS p_gas_reserves,
            gx_percent_allocation            AS p_gx_percent_allocation,
            last_gas_volume                  AS p_last_gas_volume,
            last_gas_volume_date             AS p_last_gas_volume_date,
            last_injection_volume            AS p_last_injection_volume,
            last_oil_volume                  AS p_last_oil_volume,
            last_oil_volume_date             AS p_last_oil_volume_date,
            last_total_fluids_date           AS p_last_total_fluids_date,
            last_total_fluids_volume         AS p_last_total_fluids_volume,
            last_water_volume                AS p_last_water_volume,
            last_water_volume_date           AS p_last_water_volume_date,
            max_gas_volume                   AS p_max_gas_volume,
            max_injection_volume             AS p_max_injection_volume,
            max_oil_volume                   AS p_max_oil_volume,
            max_total_fluids_volume          AS p_max_total_fluids_volume,
            max_water_volume                 AS p_max_water_volume,
            min_gas_volume                   AS p_min_gas_volume,
            min_injection_volume             AS p_min_injection_volume,
            min_oil_volume                   AS p_min_oil_volume,
            min_total_fluids_volume          AS p_min_total_fluids_volume,
            min_water_volume                 AS p_min_water_volume,
            oil_eur                          AS p_oil_eur,
            oil_reserves                     AS p_oil_reserves,
            row_changed_date                 AS p_row_changed_date,
            uwi                              AS p_uwi,
            volume_method                    AS p_volume_method,
            zone_id                          AS p_zone_id
        FROM well_cumulative_production
    ),
    m AS (
        SELECT
            uwi                            AS id_m_uwi,
            zone_id                        AS id_m_zone_id,
            activity_type                  AS id_m_activity_type,
            volume_method                  AS id_m_volume_method,
            MAX(row_changed_date)          AS max_row_changed_date,
            LIST(IFNULL(pden_date,             '{PURR_NULL}', CAST(pden_date AS VARCHAR)),             '{PURR_DELIM}' ORDER BY pden_date) AS m_pden_date,
            LIST(IFNULL(pden_source,           '{PURR_NULL}', pden_source),                            '{PURR_DELIM}' ORDER BY pden_date) AS m_pden_source,
            LIST(IFNULL(gas_volume,            '{PURR_NULL}', CAST(gas_volume AS VARCHAR)),            '{PURR_DELIM}' ORDER BY pden_date) AS m_gas_volume,
            LIST(IFNULL(gx_percent_allocation, '{PURR_NULL}', CAST(gx_percent_allocation AS VARCHAR)), '{PURR_DELIM}' ORDER BY pden_date) AS m_gx_percent_allocation,
            LIST(IFNULL(oil_volume,            '{PURR_NULL}', CAST(oil_volume AS VARCHAR)),            '{PURR_DELIM}' ORDER BY pden_date) AS m_oil_volume,
            LIST(IFNULL(prod_time,             '{PURR_NULL}', CAST(prod_time AS VARCHAR)),             '{PURR_DELIM}' ORDER BY pden_date) AS m_prod_time,
            LIST(IFNULL(row_changed_date,      '{PURR_NULL}', CAST(row_changed_date AS VARCHAR)),      '{PURR_DELIM}' ORDER BY pden_date) AS m_row_changed_date,
            LIST(IFNULL(volume_month,          '{PURR_NULL}', CAST(volume_month AS VARCHAR)),          '{PURR_DELIM}' ORDER BY pden_date) AS m_volume_month,
            LIST(IFNULL(volume_year,           '{PURR_NULL}', CAST(volume_year AS VARCHAR)),           '{PURR_DELIM}' ORDER BY pden_date) AS m_volume_year,
            LIST(IFNULL(water_volume,          '{PURR_NULL}', CAST(water_volume AS VARCHAR)),          '{PURR_DELIM}' ORDER BY pden_date) AS m_water_volume
        FROM gx_pden_vol_sum_by_month  
        GROUP BY uwi, zone_id, activity_type, volume_method
    )
    SELECT
        w.*,
        p.*,
        m.*
    FROM w
    JOIN p ON
        p.p_uwi = w.w_uwi
    JOIN m ON
        m.id_m_uwi = w.w_uwi AND
        m.id_m_zone_id = p.p_zone_id AND
        m.id_m_activity_type = p.p_activity_type AND
        m.id_m_volume_method = p.p_volume_method
    {PURR_WHERE}
    """

# maybe use well_cumulative_production, but not sure if trigger is 100% valid
identifier = f"""
    SELECT
        DISTINCT w.uwi AS w_uwi
    FROM well w
    JOIN gx_pden_vol_sum_by_month g
        ON w.uwi = g.uwi
    {PURR_WHERE}
    """
recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "p_": "well_cumulative_production",
        "m_": "gx_pden_vol_sum_by_month",
    },
    "xforms": {
        "m_pden_date": "array_of_datetime",
        "m_pden_source": "array_of_string",
        "m_gas_volume": "array_of_float",
        "m_gx_percent_allocation": "array_of_float",
        "m_oil_volume": "array_of_float",
        "m_prod_time": "array_of_float",
        "m_row_changed_date": "array_of_datetime",
        "m_volume_month": "array_of_int",
        "m_volume_year": "array_of_int",
        "m_water_volume": "array_of_float",
    },
    "chunk_size": 100,
}
