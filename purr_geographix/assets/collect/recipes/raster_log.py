"""GeoGraphix raster_log"""

from purr_geographix.assets.collect.xformer import PURR_WHERE


"""
v AS (
    SELECT
    bfile_data                   AS v_bfile_data,
    blk_no                       AS v_blk_no,
    blob_data                    AS v_blob_data,
    blob_data                    AS v_blob_data_orig,
    bytes_used                   AS v_bytes_used,
    datatype                     AS v_datatype,
    ow_rel_path                  AS v_ow_rel_path,
    vec_storage_type             AS v_vec_storage_type,
    vid                          AS v_vid
FROM log_depth_cal_vec
)

JOIN v ON 
    r.r_log_depth_cal_vid = v.v_vid
"""


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
    r AS (
        SELECT
            log_section_index             AS r_log_section_index,
            base_depth                    AS r_base_depth,
            base_depth_ouom               AS r_base_depth_ouom,
            bottom_left_x_pixel           AS r_bottom_left_x_pixel,
            bottom_left_y_pixel           AS r_bottom_left_y_pixel,
            bottom_right_x_pixel          AS r_bottom_right_x_pixel,
            bottom_right_y_pixel          AS r_bottom_right_y_pixel,
            create_date                   AS r_create_date,
            create_user_id                AS r_create_user_id,
            curve_bottom_left_x_pixel     AS r_curve_bottom_left_x_pixel,
            curve_bottom_left_y_pixel     AS r_curve_bottom_left_y_pixel,
            curve_bottom_right_x_pixel    AS r_curve_bottom_right_x_pixel,
            curve_bottom_right_y_pixel    AS r_curve_bottom_right_y_pixel, 
            curve_top_left_x_pixel        AS r_curve_top_left_x_pixel,
            curve_top_left_y_pixel        AS r_curve_top_left_y_pixel,
            curve_top_right_x_pixel       AS r_curve_top_right_x_pixel,
            curve_top_right_y_pixel       AS r_curve_top_right_y_pixel,
            header_bottom_left_x_pixel    AS r_header_bottom_left_x_pixel,
            header_bottom_left_y_pixel    AS r_header_bottom_left_y_pixel,
            header_bottom_right_x_pixel   AS r_header_bottom_right_x_pixel,
            header_bottom_right_y_pixel   AS r_header_bottom_right_y_pixel, 
            header_rotation               AS r_header_rotation,
            header_top_left_x_pixel       AS r_header_top_left_x_pixel,
            header_top_left_y_pixel       AS r_header_top_left_y_pixel,
            header_top_right_x_pixel      AS r_header_top_right_x_pixel,
            header_top_right_y_pixel      AS r_header_top_right_y_pixel,
            log_depth_cal_vid             AS r_log_depth_cal_vid,
            log_section_name              AS r_log_section_name,
            num_depth_cal_pts             AS r_num_depth_cal_pts,
            remark                        AS r_remark,
            source_registration_filename  AS r_source_registration_filename,
            tif_file_identifier           AS r_tif_file_identifier,
            tif_file_path                 AS r_tif_file_path,
            tif_filename                  AS r_tif_filename,
            top_depth                     AS r_top_depth,
            top_depth_ouom                AS r_top_depth_ouom,
            top_left_x_pixel              AS r_top_left_x_pixel,
            top_left_y_pixel              AS r_top_left_y_pixel,
            top_right_x_pixel             AS r_top_right_x_pixel,
            top_right_y_pixel             AS r_top_right_y_pixel,
            update_date                   AS r_update_date,
            update_user_id                AS r_update_user_id,
            well_id                       AS r_well_id
        FROM log_image_reg_log_section
    )
    SELECT
        w.*,
        r.*
    FROM w
    JOIN r ON
        r.r_well_id = w.w_uwi
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        DISTINCT r.well_id AS w_uwi
    FROM log_image_reg_log_section r
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "v_": "log_depth_cal_vec",
        "r_": "log_image_reg_log_section",
    },
    "xforms": {
        "v_blob_data": "decode_depth_registration",
        "v_blob_data_orig": "blob_to_hex",
    },
    "post_process": "raster_log_agg",
    "chunk_size": 1000,
}
