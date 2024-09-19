"""GeoGraphix vector_log"""

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
            wellid         AS c_wellid,
            curveset       AS c_curveset,
            curvename      AS c_curvename,
            version        AS c_version,
            cmd_type       AS c_cmd_type,
            curve_uom      AS c_curve_uom,
            curve_ouom     AS c_curve_ouom,
            date_modified  AS c_date_modified,
            description    AS c_description,
            tool_type      AS c_tool_type,
            remark         AS c_remark,
            topdepth       AS c_topdepth,
            basedepth      AS c_basedepth
        FROM gx_well_curve
    ),
    s AS (
        SELECT
            wellid         AS s_wellid,
            curveset       AS s_curveset,
            topdepth       AS s_topdepth,
            basedepth      AS s_basedepth,
            depthincr      AS s_depthincr,
            log_job        AS s_log_job,
            log_trip       AS s_log_trip,
            source_file    AS s_source_file,
            remark         AS s_remark,
            type           AS s_type,
            fielddata      AS s_fielddata,
            [import date]  AS s_import_date
        FROM gx_well_curveset
    ),
    v AS (
        SELECT
            wellid        AS v_wellid,
            curveset      AS v_curveset,
            curvename     AS v_curvename,
            version       AS v_version,
            curve_values  AS v_curve_values
        FROM gx_well_curve_values
    )
    SELECT
        w.*,
        c.*,
        s.*,
        v.*
    FROM w
    JOIN c ON
        w.w_uwi = c.c_wellid
    JOIN v ON
        c.c_wellid = v.v_wellid AND 
        c.c_curveset = v.v_curveset AND 
        c.c_curvename = v.v_curvename AND 
        c.c_version = v.v_version
    JOIN s ON
    c.c_wellid = s.s_wellid AND 
    c.c_curveset = s.s_curveset
    {PURR_WHERE}
    """


identifier = f"""
    SELECT
        DISTINCT g.wellid AS w_uwi
    FROM gx_well_curve g
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "c_": "gx_well_curve",
        "s_": "gx_well_curveset",
        "v_": "gx_well_curve_values",
    },
    "post_process": "vector_log_agg",
    "xforms": {"v_curve_values": "decode_curve_values"},
    "chunk_size": 1000,
}
