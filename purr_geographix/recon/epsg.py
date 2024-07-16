import os
import re
import xml.etree.ElementTree
import xml.etree.ElementTree as ET
from purr_geographix.core.logger import logger

geodetics = [
    ("gcs_north_american_1927", "d_north_american_1927", 4267),
    ("gcs_north_american_1983", "d_north_american_1983", 4269),
    ("gcs_wgs_1984", "d_wgs_1984", 4236),
    ("gcs_wgs_1972", "d_wgs_1972", 6322),
    ("geographic_latitude_longitude", "cape_canaveral", 6717),
    ("geographic_latitude_longitude", "itrf_1997_0", 6655),
    ("geographic_latitude_longitude", "itrf_1994", 6653),
    ("gcs_clarke_1866", "d_clarke_1866", 4008),
]

projections = [
    ("spcs27__alabama_east", "gcs_north_american_1927", 26729),
    ("spcs27__alabama_west", "gcs_north_american_1927", 26730),
    ("spcs27__arizona_central", "gcs_north_american_1927", 26749),
    ("spcs27__arizona_east", "gcs_north_american_1927", 26748),
    ("spcs27__arizona_west", "gcs_north_american_1927", 26750),
    ("spcs27__arkansas_north", "gcs_north_american_1927", 26751),
    ("spcs27__arkansas_south", "gcs_north_american_1927", 26752),
    ("spcs27__california_i", "gcs_north_american_1927", 26741),
    ("spcs27__california_ii", "gcs_north_american_1927", 26742),
    ("spcs27__california_iii", "gcs_north_american_1927", 26743),
    ("spcs27__california_iv", "gcs_north_american_1927", 26744),
    ("spcs27__california_v", "gcs_north_american_1927", 26745),
    ("spcs27__california_vi", "gcs_north_american_1927", 26746),
    ("spcs27__california_vii", "gcs_north_american_1927", 26799),
    ("spcs27__colorado_central", "gcs_north_american_1927", 26754),
    ("spcs27__colorado_north", "gcs_north_american_1927", 26753),
    ("spcs27__colorado_south", "gcs_north_american_1927", 26755),
    ("spcs27__connecticut", "gcs_north_american_1927", 26756),
    ("spcs27__delaware", "gcs_north_american_1927", 26757),
    ("spcs27__florida_east", "gcs_north_american_1927", 26758),
    ("spcs27__florida_north", "gcs_north_american_1927", 26760),
    ("spcs27__florida_west", "gcs_north_american_1927", 26759),
    ("spcs27__georgia_east", "gcs_north_american_1927", 26766),
    ("spcs27__georgia_west", "gcs_north_american_1927", 26767),
    ("spcs27__idaho_central", "gcs_north_american_1927", 26769),
    ("spcs27__idaho_east", "gcs_north_american_1927", 26768),
    ("spcs27__idaho_west", "gcs_north_american_1927", 26770),
    ("spcs27__illinois_east", "gcs_north_american_1927", 26771),
    ("spcs27__illinois_west", "gcs_north_american_1927", 26772),
    ("spcs27__indiana_east", "gcs_north_american_1927", 26773),
    ("spcs27__indiana_west", "gcs_north_american_1927", 26774),
    ("spcs27__iowa_north", "gcs_north_american_1927", 26775),
    ("spcs27__iowa_south", "gcs_north_american_1927", 26776),
    ("spcs27__kansas_north", "gcs_north_american_1927", 26777),
    ("spcs27__kansas_south", "gcs_north_american_1927", 26778),
    ("spcs27__kentucky_north", "gcs_north_american_1927", 26779),
    ("spcs27__kentucky_south", "gcs_north_american_1927", 26780),
    ("spcs27__louisiana_north", "gcs_north_american_1927", 26781),
    ("spcs27__louisiana_offshore", "gcs_north_american_1927", 32099),
    ("spcs27__louisiana_south", "gcs_north_american_1927", 26782),
    ("spcs27__maine_east", "gcs_north_american_1927", 26783),
    ("spcs27__maine_west", "gcs_north_american_1927", 26784),
    ("spcs27__maryland", "gcs_north_american_1927", 26785),
    ("spcs27__massachusetts_island", "gcs_north_american_1927", 26787),
    ("spcs27__massachusetts_mainland", "gcs_north_american_1927", 26786),
    ("spcs27__michigan_central_lambert", "gcs_north_american_1927", 6201),
    ("spcs27__michigan_central_tm_obsolete", "gcs_north_american_1927", 5624),
    ("spcs27__michigan_east_tm_obsolete", "gcs_north_american_1927", 5623),
    ("spcs27__michigan_north_lambert", "gcs_north_american_1927", 6966),
    ("spcs27__michigan_south_lambert", "gcs_north_american_1927", 6202),
    ("spcs27__michigan_west_tm_obsolete", "gcs_north_american_1927", 5625),
    ("spcs27__minnesota_central", "gcs_north_american_1927", 26792),
    ("spcs27__minnesota_north", "gcs_north_american_1927", 26791),
    ("spcs27__minnesota_south", "gcs_north_american_1927", 26793),
    ("spcs27__mississippi_east", "gcs_north_american_1927", 26794),
    ("spcs27__mississippi_west", "gcs_north_american_1927", 26795),
    ("spcs27__missouri_central", "gcs_north_american_1927", 26797),
    ("spcs27__missouri_east", "gcs_north_american_1927", 26796),
    ("spcs27__missouri_west", "gcs_north_american_1927", 26798),
    ("spcs27__montana_central", "gcs_north_american_1927", 32002),
    ("spcs27__montana_north", "gcs_north_american_1927", 32001),
    ("spcs27__montana_south", "gcs_north_american_1927", 32003),
    ("spcs27__nebraska_north", "gcs_north_american_1927", 32005),
    ("spcs27__nebraska_south", "gcs_north_american_1927", 32006),
    ("spcs27__nevada_central", "gcs_north_american_1927", 32008),
    ("spcs27__nevada_east", "gcs_north_american_1927", 32007),
    ("spcs27__nevada_west", "gcs_north_american_1927", 32009),
    ("spcs27__new_hampshire", "gcs_north_american_1927", 32010),
    ("spcs27__new_jersey", "gcs_north_american_1927", 32011),
    ("spcs27__new_mexico_central", "gcs_north_american_1927", 32013),
    ("spcs27__new_mexico_east", "gcs_north_american_1927", 32012),
    ("spcs27__new_mexico_west", "gcs_north_american_1927", 32014),
    ("spcs27__new_york_central", "gcs_north_american_1927", 32016),
    ("spcs27__new_york_east", "gcs_north_american_1927", 32015),
    ("spcs27__new_york_long_island", "gcs_north_american_1927", 4456),
    ("spcs27__new_york_west", "gcs_north_american_1927", 32017),
    ("spcs27__north_carolina", "gcs_north_american_1927", 32019),
    ("spcs27__north_dakota_north", "gcs_north_american_1927", 32020),
    ("spcs27__north_dakota_south", "gcs_north_american_1927", 32021),
    ("spcs27__ohio_north", "gcs_north_american_1927", 32022),
    ("spcs27__ohio_south", "gcs_north_american_1927", 32023),
    ("spcs27__oklahoma_north", "gcs_north_american_1927", 32024),
    ("spcs27__oklahoma_south", "gcs_north_american_1927", 32025),
    ("spcs27__oregon_north", "gcs_north_american_1927", 32026),
    ("spcs27__oregon_south", "gcs_north_american_1927", 32027),
    ("spcs27__pennsylvania_north", "gcs_north_american_1927", 32028),
    ("spcs27__pennsylvania_south", "gcs_north_american_1927", 4455),
    ("spcs27__rhode_island_south", "gcs_north_american_1927", 32030),
    ("spcs27__south_carolina_north", "gcs_north_american_1927", 32031),
    ("spcs27__south_carolina_south", "gcs_north_american_1927", 32033),
    ("spcs27__south_dakota_north", "gcs_north_american_1927", 32034),
    ("spcs27__south_dakota_south", "gcs_north_american_1927", 32035),
    ("spcs27__tennessee", "gcs_north_american_1927", 2204),
    ("spcs27__texas_central", "gcs_north_american_1927", 32039),
    ("spcs27__texas_north", "gcs_north_american_1927", 32037),
    ("spcs27__texas_north_central", "gcs_north_american_1927", 32038),
    ("spcs27__texas_south", "gcs_north_american_1927", 32041),
    ("spcs27__texas_south_central", "gcs_north_american_1927", 32040),
    ("spcs27__utah_central", "gcs_north_american_1927", 32043),
    ("spcs27__utah_north", "gcs_north_american_1927", 32042),
    ("spcs27__utah_south", "gcs_north_american_1927", 32044),
    ("spcs27__vermont", "gcs_north_american_1927", 32045),
    ("spcs27__virginia_north", "gcs_north_american_1927", 32046),
    ("spcs27__virginia_south", "gcs_north_american_1927", 32047),
    ("spcs27__washington_north", "gcs_north_american_1927", 32048),
    ("spcs27__washington_south", "gcs_north_american_1927", 32049),
    ("spcs27__west_virginia_north", "gcs_north_american_1927", 32050),
    ("spcs27__west_virginia_south", "gcs_north_american_1927", 32051),
    ("spcs27__wisconsin_central", "gcs_north_american_1927", 32053),
    ("spcs27__wisconsin_north", "gcs_north_american_1927", 32052),
    ("spcs27__wisconsin_south", "gcs_north_american_1927", 32054),
    ("spcs27__wyoming_east", "gcs_north_american_1927", 32055),
    ("spcs27__wyoming_east_central", "gcs_north_american_1927", 32056),
    ("spcs27__wyoming_west", "gcs_north_american_1927", 32058),
    ("spcs27__wyoming_west_central", "gcs_north_american_1927", 32057),
    ("utm_zone_1_north_180_w__174_w_longitude", "gcs_north_american_1927", 26701),
    ("utm_zone_2_north_174_w__168_w_longitude", "gcs_north_american_1927", 26702),
    ("utm_zone_3_north_168_w__162_w_longitude", "gcs_north_american_1927", 26703),
    ("utm_zone_4_north_162_w__156_w_longitude", "gcs_north_american_1927", 26704),
    ("utm_zone_5_north_156_w__150_w_longitude", "gcs_north_american_1927", 26705),
    ("utm_zone_6_north_150_w__144_w_longitude", "gcs_north_american_1927", 26706),
    ("utm_zone_7_north_144_w__138_w_longitude", "gcs_north_american_1927", 26707),
    ("utm_zone_8_north_138_w__132_w_longitude", "gcs_north_american_1927", 26708),
    ("utm_zone_9_north_132_w__126_w_longitude", "gcs_north_american_1927", 26709),
    ("utm_zone_10_north_126_w__120_w_longitude", "gcs_north_american_1927", 26710),
    ("utm_zone_11_north_120_w__114_w_longitude", "gcs_north_american_1927", 26711),
    ("utm_zone_12_north_114_w__108_w_longitude", "gcs_north_american_1927", 26712),
    ("utm_zone_13_north_108_w__102_w_longitude", "gcs_north_american_1927", 26713),
    ("utm_zone_14_north_102_w__96_w_longitude", "gcs_north_american_1927", 26714),
    ("utm_zone_15_north_96_w__90_w_longitude", "gcs_north_american_1927", 26715),
    ("utm_zone_16_north_90_w__84_w_longitude", "gcs_north_american_1927", 26716),
    ("utm_zone_17_north_84_w__78_w_longitude", "gcs_north_american_1927", 26717),
    ("utm_zone_18_north_78_w__72_w_longitude", "gcs_north_american_1927", 26718),
    ("utm_zone_19_north_72_w__66_w_longitude", "gcs_north_american_1927", 26719),
    (
        "utm_zone_1__u_s__survey_feet_180_w__174_w_longitude",
        "gcs_north_american_1927",
        4401,
    ),
    (
        "utm_zone_2__u_s__survey_feet_174_w__168_w_longitude",
        "gcs_north_american_1927",
        4402,
    ),
    (
        "utm_zone_3__u_s__survey_feet_168_w__162_w_longitude",
        "gcs_north_american_1927",
        4403,
    ),
    (
        "utm_zone_4__u_s__survey_feet_162_w__156_w_longitude",
        "gcs_north_american_1927",
        4404,
    ),
    (
        "utm_zone_5__u_s__survey_feet_156_w__150_w_longitude",
        "gcs_north_american_1927",
        4405,
    ),
    (
        "utm_zone_6__u_s__survey_feet_150_w__144_w_longitude",
        "gcs_north_american_1927",
        4406,
    ),
    (
        "utm_zone_7__u_s__survey_feet_144_w__138_w_longitude",
        "gcs_north_american_1927",
        4407,
    ),
    (
        "utm_zone_8__u_s__survey_feet_138_w__132_w_longitude",
        "gcs_north_american_1927",
        4408,
    ),
    (
        "utm_zone_9__u_s__survey_feet_132_w__126_w_longitude",
        "gcs_north_american_1927",
        4409,
    ),
    (
        "utm_zone_10__u_s__survey_feet_126_w__120_w_longitude",
        "gcs_north_american_1927",
        4410,
    ),
    (
        "utm_zone_11__u_s__survey_feet_120_w__114_w_longitude",
        "gcs_north_american_1927",
        4411,
    ),
    (
        "utm_zone_12__u_s__survey_feet_114_w__108_w_longitude",
        "gcs_north_american_1927",
        4412,
    ),
    (
        "utm_zone_13__u_s__survey_feet_108_w__102_w_longitude",
        "gcs_north_american_1927",
        4413,
    ),
    (
        "utm_zone_14__u_s__survey_feet_102_w__96_w_longitude",
        "gcs_north_american_1927",
        4414,
    ),
    (
        "utm_zone_15__u_s__survey_feet_96_w__90_w_longitude",
        "gcs_north_american_1927",
        4415,
    ),
    (
        "utm_zone_16__u_s__survey_feet_90_w__84_w_longitude",
        "gcs_north_american_1927",
        4416,
    ),
    (
        "utm_zone_17__u_s__survey_feet_84_w__78_w_longitude",
        "gcs_north_american_1927",
        4417,
    ),
    (
        "utm_zone_18__u_s__survey_feet_78_w__72_w_longitude",
        "gcs_north_american_1927",
        4418,
    ),
    (
        "utm_zone_19__u_s__survey_feet_72_w__66_w_longitude",
        "gcs_north_american_1927",
        4419,
    ),
    ("state_plane_1927__alaska_zone_1", "gcs_north_american_1927", 26731),
    ("state_plane_1927__alaska_zone_2", "gcs_north_american_1927", 26732),
    ("state_plane_1927__alaska_zone_3", "gcs_north_american_1927", 26733),
    ("state_plane_1927__alaska_zone_4", "gcs_north_american_1927", 26734),
    ("state_plane_1927__alaska_zone_5", "gcs_north_american_1927", 26735),
    ("state_plane_1927__alaska_zone_6", "gcs_north_american_1927", 26736),
    ("state_plane_1927__alaska_zone_7", "gcs_north_american_1927", 26737),
    ("state_plane_1927__alaska_zone_8", "gcs_north_american_1927", 26738),
    ("state_plane_1927__alaska_zone_9", "gcs_north_american_1927", 26739),
    ("state_plane_1927__alaska_zone_10", "gcs_north_american_1927", 26740),
    ("spcs83__alabama_east", "gcs_north_american_1983", 26929),
    ("spcs83__alabama_west", "gcs_north_american_1983", 26930),
    ("spcs83__arizona_central", "gcs_north_american_1983", 26949),
    ("spcs83__arizona_east", "gcs_north_american_1983", 26948),
    ("spcs83__arizona_west", "gcs_north_american_1983", 26950),
    ("spcs83__arkansas_north", "gcs_north_american_1983", 26951),
    ("spcs83__arkansas_south", "gcs_north_american_1983", 26952),
    ("spcs83__california_i", "gcs_north_american_1983", 26941),
    ("spcs83__california_ii", "gcs_north_american_1983", 26942),
    ("spcs83__california_iii", "gcs_north_american_1983", 26943),
    ("spcs83__california_iv", "gcs_north_american_1983", 26944),
    ("spcs83__california_v", "gcs_north_american_1983", 26945),
    ("spcs83__california_vi", "gcs_north_american_1983", 26946),
    ("spcs83__colorado_central", "gcs_north_american_1983", 26954),
    ("spcs83__colorado_north", "gcs_north_american_1983", 26953),
    ("spcs83__colorado_south", "gcs_north_american_1983", 26955),
    ("spcs83__connecticut", "gcs_north_american_1983", 26956),
    ("spcs83__delaware", "gcs_north_american_1983", 26957),
    ("spcs83__florida_east", "gcs_north_american_1983", 26958),
    ("spcs83__florida_north", "gcs_north_american_1983", 26960),
    ("spcs83__florida_west", "gcs_north_american_1983", 26959),
    ("spcs83__georgia_east", "gcs_north_american_1983", 26966),
    ("spcs83__georgia_west", "gcs_north_american_1983", 26967),
    ("spcs83__idaho_central", "gcs_north_american_1927", 26969),
    ("spcs83__idaho_east", "gcs_north_american_1927", 26968),
    ("spcs83__idaho_west", "gcs_north_american_1927", 26970),
    ("spcs83__illinois_east", "gcs_north_american_1983", 26971),
    ("spcs83__illinois_west", "gcs_north_american_1983", 26972),
    ("spcs83__indiana_east", "gcs_north_american_1983", 26973),
    ("spcs83__indiana_west", "gcs_north_american_1983", 26974),
    ("spcs83__iowa_north", "gcs_north_american_1983", 26975),
    ("spcs83__iowa_south", "gcs_north_american_1983", 26976),
    ("spcs83__kansas_north", "gcs_north_american_1983", 26977),
    ("spcs83__kansas_south", "gcs_north_american_1983", 26978),
    ("spcs83__kentucky_north", "gcs_north_american_1983", 2205),
    ("spcs83__kentucky_south", "gcs_north_american_1983", 26980),
    ("spcs83__louisiana_north", "gcs_north_american_1983", 26981),
    ("spcs83__louisiana_offshore", "gcs_north_american_1983", 32199),
    ("spcs83__louisiana_south", "gcs_north_american_1983", 26982),
    ("spcs83__maine_east_gcs", "north_american_1983", 26983),
    ("spcs83__maine_west_gcs", "north_american_1983", 26984),
    ("spcs83__maryland_gcs", "north_american_1983", 26985),
    ("spcs83__massachusetts_island", "gcs_north_american_1983", 26987),
    ("spcs83__massachusetts_mainland", "gcs_north_american_1983", 26986),
    ("spcs83__michigan_central_lambert", "gcs_north_american_1983", 26989),
    ("spcs83__michigan_north_lambert", "gcs_north_american_1983", 26988),
    ("spcs83__michigan_south_lambert", "gcs_north_american_1983", 26990),
    ("spcs83__minnesota_central", "gcs_north_american_1983", 26992),
    ("spcs83__minnesota_north", "gcs_north_american_1983", 26991),
    ("spcs83__minnesota_south", "gcs_north_american_1983", 26993),
    ("spcs83__mississippi_east", "gcs_north_american_1983", 26994),
    ("spcs83__mississippi_west", "gcs_north_american_1983", 26995),
    ("spcs83__missouri_central", "gcs_north_american_1983", 26997),
    ("spcs83__missouri_east", "gcs_north_american_1983", 26996),
    ("spcs83__missouri_west", "gcs_north_american_1983", 26998),
    ("spcs83__montana", "gcs_north_american_1983", 32100),
    ("spcs83__nebraska", "gcs_north_american_1983", 32104),
    ("spcs83__nevada_central", "gcs_north_american_1983", 32108),
    ("spcs83__nevada_east", "gcs_north_american_1983", 32107),
    ("spcs83__nevada_west", "gcs_north_american_1983", 32109),
    ("spcs83__new_hampshire", "gcs_north_american_1983", 32110),
    ("spcs83__new_jersey", "gcs_north_american_1983", 32111),
    ("spcs83__new_mexico_central", "gcs_north_american_1983", 32113),
    ("spcs83__new_mexico_east", "gcs_north_american_1983", 32112),
    ("spcs83__new_mexico_west", "gcs_north_american_1983", 32114),
    ("spcs83__new_york_central", "gcs_north_american_1983", 32116),
    ("spcs83__new_york_east", "gcs_north_american_1983", 32115),
    ("spcs83__new_york_long_island", "gcs_north_american_1983", 32118),
    ("spcs83__new_york_west", "gcs_north_american_1983", 32117),
    ("spcs83__north_carolina", "gcs_north_american_1983", 32119),
    ("spcs83__north_dakota_north", "gcs_north_american_1983", 32120),
    ("spcs83__north_dakota_south", "gcs_north_american_1983", 32121),
    ("spcs83__ohio_north", "gcs_north_american_1983", 32122),
    ("spcs83__ohio_south", "gcs_north_american_1983", 32123),
    ("spcs83__oklahoma_north", "gcs_north_american_1983", 32124),
    ("spcs83__oklahoma_south", "gcs_north_american_1983", 32125),
    ("spcs83__oregon_north", "gcs_north_american_1983", 32126),
    ("spcs83__oregon_south", "gcs_north_american_1983", 32127),
    ("spcs83__pennsylvania_north", "gcs_north_american_1983", 32128),
    ("spcs83__pennsylvania_south", "gcs_north_american_1983", 32129),
    ("spcs83__rhode_island_south", "gcs_north_american_1983", 32130),
    ("spcs83__south_carolina", "gcs_north_american_1983", 32133),
    ("spcs83__south_dakota_north", "gcs_north_american_1983", 32134),
    ("spcs83__south_dakota_south", "gcs_north_american_1983", 32125),
    ("spcs83__tennessee", "gcs_north_american_1983", 32136),
    ("spcs83__texas_central", "gcs_north_american_1983", 32139),
    ("spcs83__texas_north", "gcs_north_american_1983", 32137),
    ("spcs83__texas_north_central", "gcs_north_american_1983", 32138),
    ("spcs83__texas_south", "gcs_north_american_1983", 32141),
    ("spcs83__texas_south_central", "gcs_north_american_1983", 32140),
    ("spcs83__utah_central", "gcs_north_american_1983", 32143),
    ("spcs83__utah_north", "gcs_north_american_1983", 32142),
    ("spcs83__utah_south", "gcs_north_american_1983", 32144),
    ("spcs83__vermont", "gcs_north_american_1983", 32145),
    ("spcs83__virginia_north", "gcs_north_american_1983", 32146),
    ("spcs83__virginia_south", "gcs_north_american_1983", 32147),
    ("spcs83__washington_north", "gcs_north_american_1983", 32148),
    ("spcs83__washington_south", "gcs_north_american_1983", 32149),
    ("spcs83__west_virginia_north", "gcs_north_american_1983", 32150),
    ("spcs83__west_virginia_south", "gcs_north_american_1983", 32151),
    ("spcs83__wisconsin_central", "gcs_north_american_1983", 32153),
    ("spcs83__wisconsin_north", "gcs_north_american_1983", 32152),
    ("spcs83__wisconsin_south", "gcs_north_american_1983", 32154),
    ("spcs83__wyoming_east", "gcs_north_american_1983", 32155),
    ("spcs83__wyoming_east_central", "gcs_north_american_1983", 32156),
    ("spcs83__wyoming_west", "gcs_north_american_1983", 32158),
    ("spcs83__wyoming_west_central", "gcs_north_american_1983", 32157),
    ("utm_zone_1_north_180_w__174_w_longitude", "gcs_north_american_1983", 26901),
    ("utm_zone_2_north_174_w__168_w_longitude", "gcs_north_american_1983", 26902),
    ("utm_zone_3_north_168_w__162_w_longitude", "gcs_north_american_1983", 26903),
    ("utm_zone_4_north_162_w__156_w_longitude", "gcs_north_american_1983", 26904),
    ("utm_zone_5_north_156_w__150_w_longitude", "gcs_north_american_1983", 26905),
    ("utm_zone_6_north_150_w__144_w_longitude", "gcs_north_american_1983", 26906),
    ("utm_zone_7_north_144_w__138_w_longitude", "gcs_north_american_1983", 26907),
    ("utm_zone_8_north_138_w__132_w_longitude", "gcs_north_american_1983", 26908),
    ("utm_zone_9_north_132_w__126_w_longitude", "gcs_north_american_1983", 26909),
    ("utm_zone_10_north_126_w__120_w_longitude", "gcs_north_american_1983", 26910),
    ("utm_zone_11_north_120_w__114_w_longitude", "gcs_north_american_1983", 26911),
    ("utm_zone_12_north_114_w__108_w_longitude", "gcs_north_american_1983", 26912),
    ("utm_zone_13_north_108_w__102_w_longitude", "gcs_north_american_1983", 26913),
    ("utm_zone_14_north_102_w__96_w_longitude", "gcs_north_american_1983", 26914),
    ("utm_zone_15_north_96_w__90_w_longitude", "gcs_north_american_1983", 26915),
    ("utm_zone_16_north_90_w__84_w_longitude", "gcs_north_american_1983", 26916),
    ("utm_zone_17_north_84_w__78_w_longitude", "gcs_north_american_1983", 26917),
    ("utm_zone_18_north_78_w__72_w_longitude", "gcs_north_american_1983", 26918),
    ("utm_zone_19_north_72_w__66_w_longitude", "gcs_north_american_1983", 26919),
    (
        "utm_zone_1__u_s__survey_feet_180_w__174_w_longitude",
        "gcs_north_american_1983",
        4421,
    ),
    (
        "utm_zone_2__u_s__survey_feet_174_w__168_w_longitude",
        "gcs_north_american_1983",
        4422,
    ),
    (
        "utm_zone_3__u_s__survey_feet_168_w__162_w_longitude",
        "gcs_north_american_1983",
        4423,
    ),
    (
        "utm_zone_4__u_s__survey_feet_162_w__156_w_longitude",
        "gcs_north_american_1983",
        4424,
    ),
    (
        "utm_zone_5__u_s__survey_feet_156_w__150_w_longitude",
        "gcs_north_american_1983",
        4425,
    ),
    (
        "utm_zone_6__u_s__survey_feet_150_w__144_w_longitude",
        "gcs_north_american_1983",
        4426,
    ),
    (
        "utm_zone_7__u_s__survey_feet_144_w__138_w_longitude",
        "gcs_north_american_1983",
        4427,
    ),
    (
        "utm_zone_8__u_s__survey_feet_138_w__132_w_longitude",
        "gcs_north_american_1983",
        4428,
    ),
    (
        "utm_zone_9__u_s__survey_feet_132_w__126_w_longitude",
        "gcs_north_american_1983",
        4429,
    ),
    (
        "utm_zone_10__u_s__survey_feet_126_w__120_w_longitude",
        "gcs_north_american_1983",
        4430,
    ),
    (
        "utm_zone_11__u_s__survey_feet_120_w__114_w_longitude",
        "gcs_north_american_1983",
        4431,
    ),
    (
        "utm_zone_12__u_s__survey_feet_114_w__108_w_longitude",
        "gcs_north_american_1983",
        4432,
    ),
    (
        "utm_zone_13__u_s__survey_feet_108_w__102_w_longitude",
        "gcs_north_american_1983",
        4433,
    ),
    (
        "utm_zone_14__u_s__survey_feet_102_w__96_w_longitude",
        "gcs_north_american_1983",
        4434,
    ),
    (
        "utm_zone_15__u_s__survey_feet_96_w__90_w_longitude",
        "gcs_north_american_1983",
        4435,
    ),
    (
        "utm_zone_16__u_s__survey_feet_90_w__84_w_longitude",
        "gcs_north_american_1983",
        4436,
    ),
    (
        "utm_zone_17__u_s__survey_feet_84_w__78_w_longitude",
        "gcs_north_american_1983",
        4437,
    ),
    (
        "utm_zone_18__u_s__survey_feet_78_w__72_w_longitude",
        "gcs_north_american_1983",
        4438,
    ),
    (
        "utm_zone_19__u_s__survey_feet_72_w__66_w_longitude",
        "gcs_north_american_1983",
        4439,
    ),
    ("state_plane_1983__alaska_zone_1", "gcs_north_american_1983", 26931),
    ("state_plane_1983__alaska_zone_2", "gcs_north_american_1983", 26932),
    ("state_plane_1983__alaska_zone_3", "gcs_north_american_1983", 26933),
    ("state_plane_1983__alaska_zone_4", "gcs_north_american_1983", 26934),
    ("state_plane_1983__alaska_zone_5", "gcs_north_american_1983", 26935),
    ("state_plane_1983__alaska_zone_6", "gcs_north_american_1983", 26936),
    ("state_plane_1983__alaska_zone_7", "gcs_north_american_1983", 26937),
    ("state_plane_1983__alaska_zone_8", "gcs_north_american_1983", 26938),
    ("state_plane_1983__alaska_zone_9", "gcs_north_american_1983", 26939),
    ("state_plane_1983__alaska_zone_10", "gcs_north_american_1983", 26940),
    ("utm_zone_1_north_180_w__174_w_longitude", "gcs_wgs_1984", 32601),
    ("utm_zone_2_north_174_w__168_w_longitude", "gcs_wgs_1984", 32602),
    ("utm_zone_3_north_168_w__162_w_longitude", "gcs_wgs_1984", 32603),
    ("utm_zone_4_north_162_w__156_w_longitude", "gcs_wgs_1984", 32604),
    ("utm_zone_5_north_156_w__150_w_longitude", "gcs_wgs_1984", 32605),
    ("utm_zone_6_north_150_w__144_w_longitude", "gcs_wgs_1984", 32606),
    ("utm_zone_7_north_144_w__138_w_longitude", "gcs_wgs_1984", 32607),
    ("utm_zone_8_north_138_w__132_w_longitude", "gcs_wgs_1984", 32608),
    ("utm_zone_9_north_132_w__126_w_longitude", "gcs_wgs_1984", 32609),
    ("utm_zone_10_north_126_w__120_w_longitude", "gcs_wgs_1984", 32610),
    ("utm_zone_11_north_120_w__114_w_longitude", "gcs_wgs_1984", 32611),
    ("utm_zone_12_north_114_w__108_w_longitude", "gcs_wgs_1984", 32612),
    ("utm_zone_13_north_108_w__102_w_longitude", "gcs_wgs_1984", 32613),
    ("utm_zone_14_north_102_w__96_w_longitude", "gcs_wgs_1984", 32614),
    ("utm_zone_15_north_96_w__90_w_longitude", "gcs_wgs_1984", 32615),
    ("utm_zone_16_north_90_w__84_w_longitude", "gcs_wgs_1984", 32616),
    ("utm_zone_17_north_84_w__78_w_longitude", "gcs_wgs_1984", 32617),
    ("utm_zone_18_north_78_w__72_w_longitude", "gcs_wgs_1984", 32618),
    ("utm_zone_19_north_72_w__66_w_longitude", "gcs_wgs_1984", 32619),
    ("albers_equal_area_conic_usgs_dlg_1_to_2m", "gcs_north_american_1983", 102008),
    ("albers_equal_area_conic_usgs_dlg_1_to_2m", "gcs_north_american_1927", 5069),
    ("world__mercator", "gcs_north_american_1983", 3395),
    ("new_brunswick", "gcs_north_american_1927", 5588),
    ("prince_edward_island", "gcs_north_american_1927", 2290),
]


def scrub(s: str) -> str:
    """
    Utility method to (slightly) clean up epsg display text
    :param s: input string
    :return: sanitized string
    """
    underscored = re.sub(r"[ ./_]", "_", str(s))
    return re.sub(r"[-()]", "", underscored)


def get_wkts(fs_path: str) -> dict:
    """
    Extract storage and display string "Well Known Text" from GeoGraphix
    Project.ggx.xml files. Very old projects won't have the .xml file.
    :param fs_path: Path to project repo
    :return: storage and display WKT
    """
    try:
        ggx_xml = os.path.join(fs_path, "Project.ggx.xml")
        root = ET.parse(ggx_xml).getroot()

        storage_wkt = root.find("./Project/StorageCoordinateSystem/ESRI").text
        display_wkt = root.find("./Project/DisplayCoordinateSystem/ESRI").text

        return {
            "storage_wkt": scrub(storage_wkt),
            "display_wkt": scrub(display_wkt),
        }
    except FileNotFoundError as fnfe:
        logger.error(f"epsg missing file (probably Project.ggx.xml): {fnfe}")
    except xml.etree.ElementTree.ParseError as pe:
        logger.error(f"epsg XML parse error: {pe}")
    except Exception as e:
        logger.error(f"Mystery epsg error: {e}")

    return {"storage_wkt": "unknown", "display_wkt": "unknown"}


def epsg_codes(repo_base) -> dict:
    """
    Look up storage and display EPSG code and name. Mostly based on epsg.io
    :param repo_base: A stub repo dict. We just use the fs_path
    :return: ESPG names and codes
    """

    logger.info(f"epsg_codes: {repo_base['fs_path']}")

    storage_epsg = 0
    storage_name = "unknown"
    display_epsg = 0
    display_name = "unknown"

    wkt = get_wkts(repo_base["fs_path"])

    s_geog = re.search(r"(?<=GEOGCS\[\").+?(?=\",)", wkt.get("storage_wkt"))
    stor_geog = s_geog[0].lower() if s_geog is not None else None

    s_datum = re.search(r"(?<=DATUM\[\").+?(?=\",)", wkt.get("storage_wkt"))
    stor_datum = s_datum[0].lower() if s_datum is not None else None

    d_geog = re.search(r"(?<=GEOGCS\[\").+?(?=\",)", wkt.get("display_wkt"))
    disp_geog = d_geog[0].lower() if d_geog is not None else None

    d_proj = re.search(r"(?<=PROJCS\[\").+?(?=\",)", wkt.get("display_wkt"))
    disp_proj = d_proj[0].lower() if d_proj is not None else None

    for geog, datum, code in geodetics:
        if geog == stor_geog and datum == stor_datum:
            storage_epsg = code
            storage_name = geog
            break

    for proj, geog, code in projections:
        if geog == disp_geog and proj == disp_proj:
            display_epsg = code
            display_name = proj
            break

    return {
        "storage_epsg": storage_epsg,
        "storage_name": storage_name,
        "display_epsg": display_epsg,
        "display_name": display_name,
    }
