COMPLETION = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(uwi) FROM well_completion",
        "subconditions": ["__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "well_completion",
            "index_col": "uwi",
            "group_by": "uwi",
        }
    ],
    # "uwi_filter": "050010902%",
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "050%",
}

# Should be double-nesting based on well_core.core_id
CORE = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(uwi) FROM well_core",
        "subconditions": ["__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "well_core",
            "index_col": "uwi",
            "group_by": "uwi",
        },
        {
            "table_name": "well_core_sample_anal",
            "index_col": "uwi",
            "group_by": "uwi",
        },
    ],
    # "uwi_filter": "05057064630000",
    # "uwi_filter": "050%",
}

DST = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(uwi) FROM well_test",
        "subconditions": ["__uwi_sub__", "test_type = 'DST'"],
    },
    "rollups": [
        {
            "table_name": "well_test",
            "index_col": "uwi",
            "group_by": "uwi",
            "conditions": ["test_type = 'DST'"],
        },
        {
            "table_name": "well_test_pressure",
            "index_col": "uwi",
            "group_by": "uwi",
            "conditions": ["test_type = 'DST'"],
        },
        {
            "table_name": "well_test_recovery",
            "index_col": "uwi",
            "group_by": "uwi",
            "conditions": ["recovery_method = 'PIPE'"],
        },
        {
            "table_name": "well_test_flow",
            "index_col": "uwi",
            "group_by": "uwi",
            "conditions": ["test_type = 'DST'"],
        },
    ],
    # "uwi_filter": "050%",
    # "uwi_filter": "0500%",
}

FORMATION = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(uwi) FROM well_formation",
        "subconditions": ["__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "well_formation",
            "index_col": "uwi",
            "group_by": "uwi",
        }
    ],
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "050%",
}

IP = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(uwi) FROM well_test",
        "subconditions": ["test_type = 'IP'", "__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "well_test",
            "index_col": "uwi",
            "group_by": "uwi",
            "conditions": ["test_type = 'IP'"],
        },
        {
            "table_name": "well_test_pressure",
            "index_col": "uwi",
            "group_by": "uwi",
            "conditions": ["test_type = 'IP'"],
        },
        {
            "table_name": "well_test_recovery",
            "index_col": "uwi",
            "group_by": "uwi",
            "conditions": ["recovery_method = 'IP'"],
        },
        {
            "table_name": "well_test_flow",
            "index_col": "uwi",
            "group_by": "uwi",
            "conditions": ["test_type = 'IP'"],
        },
    ],
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "050%",
}

PERFORATION = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(uwi) FROM well_perforation",
        "subconditions": ["__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "well_perforation",
            "index_col": "uwi",
            "group_by": "uwi",
        }
    ],
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "0500%",
}

# should be double-nesting based on well_cumulative_production.zone_id
PRODUCTION = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(uwi) FROM well_cumulative_production",
        "subconditions": ["__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "well_cumulative_production",
            "index_col": "uwi",
            "group_by": "uwi",
        },
        {
            "table_name": "gx_pden_vol_sum_by_month",
            "index_col": "uwi",
            "group_by": "uwi",
        },
    ],
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "050%",
}

# NOTE: we skip log_depth_cal_vec.vid = log_image_reg_log_section.log_depth_cal_vid
RASTER_LOG = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(well_id) AS uwi FROM log_image_reg_log_section",
        "subconditions": ["__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "log_image_reg_log_section",
            "index_col": "well_id",
            "group_by": "uwi",
        },
    ],
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "050%",
}

SURVEY = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": (
            "SELECT DISTINCT(uwi) FROM well_dir_srvy_station"
            " UNION "
            "SELECT DISTINCT(uwi) FROM well_dir_proposed_srvy_station"
        ),
        "subconditions": ["__uwi_sub__"],
    },
    "singles": [
        {
            "table_name": "well_dir_srvy",
            "index_col": "uwi",
        },
        {
            "table_name": "well_dir_proposed_srvy",
            "index_col": "uwi",
        },
    ],
    "rollups": [
        {
            "table_name": "well_dir_srvy_station",
            "index_col": "uwi",
            "group_by": "uwi",
        },
        {
            "table_name": "well_dir_proposed_srvy_station",
            "index_col": "uwi",
            "group_by": "uwi",
        },
    ],
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "050%",
}

VECTOR_LOG = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT uwi FROM gx_well_curve",
        "subconditions": ["__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "gx_well_curve",
            "index_col": "wellid",
            "group_by": "uwi",
        },
        {
            "table_name": "gx_well_curveset",
            "index_col": "wellid",
            "group_by": "uwi",
        },
        {
            "table_name": "gx_well_curve_values",
            "index_col": "wellid",
            "group_by": "uwi",
        },
    ],
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "050%",
}

WELL = {
    "primary": {"table_name": "well", "index_col": "uwi"},
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "0500109%",
    # "uwi_filter": "050%",
}

ZONE = {
    "primary": {
        "table_name": "well",
        "index_col": "uwi",
        "subquery": "SELECT DISTINCT(uwi) FROM well_zone_interval",
        "subconditions": ["__uwi_sub__"],
    },
    "rollups": [
        {
            "table_name": "well_zone_interval",
            "index_col": "uwi",
            "group_by": "uwi",
        },
        {
            "table_name": "well_zone_intrvl_value",
            "index_col": "uwi",
            "group_by": "uwi",
        },
    ],
    # "uwi_filter": "050010902%",
    # "uwi_filter": "050010902%|050130657%|0504507834%",
    # "uwi_filter": "050%",
}

templates = {
    "completion": COMPLETION,
    "core": CORE,
    "dst": DST,
    "formation": FORMATION,
    "ip": IP,
    "perforation": PERFORATION,
    "production": PRODUCTION,
    "raster_log": RASTER_LOG,
    "survey": SURVEY,
    "vector_log": VECTOR_LOG,
    "well": WELL,
    "zone": ZONE,
}
