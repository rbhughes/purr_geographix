import asyncio
from pathlib import Path
from purr_geographix.core.crud import upsert_repos
from purr_geographix.core.database import get_db
from core.sqlanywhere import make_conn_params
from core.util import generate_repo_id, async_wrap
from purr_geographix.recon.epsg import epsg_codes
from purr_geographix.recon.repo_db import well_counts, get_polygon, check_gxdb
from purr_geographix.recon.repo_fs import network_repo_scan, dir_stats, repo_mod
from purr_geographix.core.schemas import Repo


async def repo_recon(recon_root: str, ggx_host: str = "localhost"):
    """
    1. get repo_paths
    2. create initial repo_base dict for each
    3. define and run augment functions to add more stuff to each repo_base
    4. validate and ensure that sqlite can digest dicts and datetime
    5. reformat repo.repo_mod to string to permit json serialization

    Args:
        recon_root: A directory path (project home) containing GeoGraphix repos
        ggx_host: Hostname of the GeoGraphix server. Defaults to "localhost" if
        not specified.

    Returns:
        List of repo dicts somewhat sanitized for export as JSON

    """
    repo_paths = await network_repo_scan(recon_root)
    repo_list = [create_repo_base(rp, ggx_host) for rp in repo_paths]

    # make another pass to verify gxdb (now that we have a conn)
    repo_list = [repo_base for repo_base in repo_list if check_gxdb(repo_base)]

    augment_funcs = [well_counts, get_polygon, epsg_codes, dir_stats, repo_mod]

    async def update_repo(repo_base):
        for func in augment_funcs:
            repo_base.update(await async_wrap(func)(repo_base))
        return repo_base

    repos = await asyncio.gather(*[update_repo(repo) for repo in repo_list])

    valid_repo_dicts = [Repo(**r).dict() for r in repos]

    db = next(get_db())
    upsert_repos(db, valid_repo_dicts)
    db.close()

    for r in valid_repo_dicts:
        r["repo_mod"] = r["repo_mod"].strftime("%Y-%m-%d %H:%M:%S")

    return valid_repo_dicts


def create_repo_base(rp: str, ggx_host: str):
    """
    See repo_recon for details
    """
    return {
        "id": generate_repo_id(rp),
        "active": True,
        "name": Path(rp).name,
        "fs_path": str(Path(rp)),
        "conn": make_conn_params(rp, ggx_host),
        "conn_aux": {"ggx_host": ggx_host},
        "suite": "geographix",
    }
