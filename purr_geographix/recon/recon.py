from purr_geographix.recon.repo_fs import network_repo_scan, dir_stats
from purr_geographix.recon.epsg import epsg_codes
from purr_geographix.common.sqlanywhere import make_conn_params
from pathlib import Path

from purr_geographix.recon.repo_db import well_counts, hull_outline

from purr_geographix.common.util import hashify


async def repo_recon(recon_root: str, ggx_host: str = "localhost"):
    repo_paths = await network_repo_scan(recon_root)
    repo_list = [
        {
            "id": hashify(str(Path(rp))),
            "name": Path(rp).name,
            "fs_path": str(Path(rp)),
            "conn": make_conn_params(rp, ggx_host),
            "conn_aux": {"ggx_host": ggx_host},
            "suite": "geographix",
        }
        for rp in repo_paths
    ]

    for repo_base in repo_list:
        for func in [
            well_counts,
            hull_outline,
            epsg_codes,
            dir_stats,
            # repo_mod,
        ]:
            md = func(repo_base)

            repo_base.update(md)

    return repo_list
    # validated_repo_dicts = [validate_repo(r).to_dict() for r in repos]
    # return validated_repo_dicts
