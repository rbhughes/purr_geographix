import numpy as np
import alphashape
from purr_geographix.core.sqlanywhere import db_exec
from purr_geographix.core.logger import logger

# from purr_geographix.core.util import debugger

NOTNULL_LONLAT = (
    "SELECT surface_longitude AS lon, surface_latitude AS lat FROM well "
    "WHERE surface_longitude IS NOT NULL and surface_latitude IS NOT NULL"
)

WELLS = "SELECT COUNT(uwi) AS tally FROM well"
WELLS_WITH_COMPLETION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_completion"

# const WELLS_WITH_COMPLETION = `
# SELECT COUNT(DISTINCT uwi) AS tally FROM (
#     SELECT uwi FROM well_completion
#     UNION
#     SELECT uwi FROM well_treatment
# ) x`;

WELLS_WITH_CORE = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_core"

WELLS_WITH_DST = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_test WHERE test_type = 'DST'"
)

WELLS_WITH_FORMATION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_formation"

# to match the selector for IP:
# SELECT count(DISTINCT uwi || source || run_number)
#   from well_test WHERE test_type ='IP'

WELLS_WITH_IP = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_test WHERE test_type = 'IP'"
)

WELLS_WITH_PERFORATION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_perforation"

WELLS_WITH_PRODUCTION = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_cumulative_production"
)

WELLS_WITH_RASTER_LOG = (
    "SELECT COUNT(DISTINCT(w.uwi)) AS tally FROM well w "
    "JOIN log_image_reg_log_section r ON r.well_id = w.uwi"
)

WELLS_WITH_SURVEY = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM ( "
    "SELECT uwi FROM well_dir_srvy_station "
    "UNION "
    "SELECT uwi FROM well_dir_proposed_srvy_station "
    ") x"
)

WELLS_WITH_VECTOR_LOG = "SELECT COUNT(DISTINCT wellid) AS tally FROM gx_well_curve"

WELLS_WITH_ZONE = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_zone_interval"


def check_gxdb(repo_base) -> bool:
    """A simple query to see if a SQLAnywhere/gxdb database is accessible

    Args:
        repo_base (dict): A stub repo dict.

    Returns:
        bool: True if connection and query were successful, otherwise False
    """
    res = db_exec(repo_base["conn"], "select db_name()")
    if isinstance(res, Exception):
        logger.warning(f"Looks like a ggx project but invalid gxdb?: {res}")
        return False
    elif isinstance(res, list):
        return True
    else:
        return False


def well_counts(repo_base) -> dict:
    """Run the SQL counts (above) for each asset data type.

    If SQLAnywhere returns an exception the count = None for that asset type

    Args:
        repo_base (dict): A stub repo dict. We just use the fs_path

    Returns:
        A dict with each count, named after the keys below
    """
    logger.info(f"well_counts: {repo_base['fs_path']}")

    counter_sql = {
        "well_count": WELLS,
        "wells_with_completion": WELLS_WITH_COMPLETION,
        "wells_with_core": WELLS_WITH_CORE,
        "wells_with_dst": WELLS_WITH_DST,
        "wells_with_formation": WELLS_WITH_FORMATION,
        "wells_with_ip": WELLS_WITH_IP,
        "wells_with_perforation": WELLS_WITH_PERFORATION,
        "wells_with_production": WELLS_WITH_PRODUCTION,
        "wells_with_raster_log": WELLS_WITH_RASTER_LOG,
        "wells_with_survey": WELLS_WITH_SURVEY,
        "wells_with_vector_log": WELLS_WITH_VECTOR_LOG,
        "wells_with_zone": WELLS_WITH_ZONE,
    }

    counts = {}

    for key, sql in counter_sql.items():
        res = db_exec(repo_base["conn"], sql)

        if isinstance(res, Exception):
            logger.error({"context": repo_base["fs_path"], "error": res})
            counts[key] = None
        else:
            counts[key] = res[0]["tally"] or 0
    return counts


def concave_hull(points, alpha=0.5):
    """Computes a concave hull of a set of points using alpha shape.

    Args:
        points (list): A list of (lon, lat) surface well locations.
        alpha (float): A parameter that controls the concaveness of the hull.
            Higher values of alpha produce more concave hulls.

    Returns:
        list: A list of (lon, lat) tuples representing the vertices of the hull.
    """
    points_array = np.array(points)
    alpha_shape = alphashape.alphashape(points_array, alpha)

    if alpha_shape.is_empty:
        concave_hull_vertices = None
    else:
        concave_hull_vertices = [list(coord) for coord in alpha_shape.exterior.coords]

    return concave_hull_vertices


def get_polygon(repo_base) -> dict:
    """Get a list of lat/lon points defining approximate project boundaries.

    I had used concave_hull (https://concave-hull.readthedocs.io/en/latest/),
    but it relies on numpy 1.26.4. The latest numpy (2.0.0) breaks it.

    This is (obviously) datum agnostic.

    The numpy + alphashape concave_hull function above was cobbled together with
    help from Claude and Perplexity. It's not as fancy, but seems faster and
    has a side effect ot excluding (most) crazy out-of-bounds surface locs.

    Args:
        repo_base (dict): A stub repo dict. We just use the fs_path

    Returns:
        dict with hull (List of points)
    """

    logger.info(f"get_polygon: {repo_base['fs_path']}")

    res = db_exec(repo_base["conn"], NOTNULL_LONLAT)

    if isinstance(res, Exception):
        logger.error({"context": repo_base["fs_path"], "error": res})
        return {"polygon": None}

    points = [[r["lon"], r["lat"]] for r in res]

    if len(points) < 3:
        logger.error(
            {
                "context": repo_base["fs_path"],
                "error": f"Too few valid Lon/Lat for hull: {repo_base["name"]}",
            }
        )
        return {"polygon": None}

    hull = concave_hull(points)
    if hull is None:
        # a weird edge case I've only seen in very old <2015 vintage projects
        logger.error(
            {
                "context": repo_base["fs_path"],
                "error": "The concave_hull was null, suggesting pre-SQLA17",
            }
        )
        return {"polygon": None}

    first_point = hull[0]
    hull.append(first_point)

    return {"polygon": hull}
