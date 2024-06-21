import asyncio
from pathlib import Path
from datetime import datetime
from purr_geographix.common.sqlanywhere import make_conn_params
from purr_geographix.common.util import hashify, async_wrap
from purr_geographix.recon.epsg import epsg_codes
from purr_geographix.recon.repo_db import well_counts, hull_outline
from purr_geographix.recon.repo_fs import network_repo_scan, dir_stats, repo_mod
from purr_geographix.api_modules.crud import upsert_repos
from purr_geographix.api_modules.database import get_db
# import uuid


from purr_geographix.common.typeish import validate_repo


async def repo_recon(recon_root: str, ggx_host: str = "localhost"):
    """
    1. get repo_paths
    2. create initial repo_base dict for each
    3. define and run augment functions to add more stuff to each repo_base
    4. validate and ensure that sqlite can digest dicts and datetime
    5. change the datetimes back to string to permit json.dumps

    :param recon_root:
    :param ggx_host:
    :return:
    """
    repo_paths = await network_repo_scan(recon_root)
    repo_list = [create_repo_base(rp, ggx_host) for rp in repo_paths]

    augment_funcs = [well_counts, hull_outline, epsg_codes, dir_stats, repo_mod]

    async def update_repo(repo_base):
        for func in augment_funcs:
            repo_base.update(await async_wrap(func)(repo_base))
        return repo_base

    repos = await asyncio.gather(*[update_repo(repo) for repo in repo_list])

    validated_repo_dicts = [validate_repo(r).to_dict() for r in repos]
    with get_db() as db:
        x = upsert_repos(db, validated_repo_dicts)
        print(x)

    return [{**d, "repo_mod": d["repo_mod"].strftime("%Y-%m-%d %H:%M:%S")}
            for d in validated_repo_dicts]



def create_repo_base(rp: str, ggx_host: str):
    return {
        "id": hashify(rp),
        "active": True,
        "name": Path(rp).name,
        "fs_path": str(Path(rp)),
        "conn": make_conn_params(rp, ggx_host),
        "conn_aux": {"ggx_host": ggx_host},
        "suite": "geographix",
    }


# async def repo_reconZ(recon_root: str, ggx_host: str = "localhost"):
#     repo_paths = await network_repo_scan(recon_root)
#     repo_list = [
#         {
#             "id": hashify(str(Path(rp))),
#             "name": Path(rp).name,
#             "fs_path": str(Path(rp)),
#             "conn": make_conn_params(rp, ggx_host),
#             "conn_aux": {"ggx_host": ggx_host},
#             "suite": "geographix",
#         }
#         for rp in repo_paths
#     ]
#
#     for repo_base in repo_list:
#         repo_base.update(await async_wrap(well_counts)(repo_base))
#         repo_base.update(await async_wrap(hull_outline)(repo_base))
#         repo_base.update(await async_wrap(epsg_codes)(repo_base))
#         repo_base.update(await async_wrap(dir_stats)(repo_base))
#         repo_base.update(await async_wrap(repo_mod)(repo_base))
#
#     # for repo_base in repo_list:
#     #     print(f"processing {repo_base}")
#     #     # await snooze(30)
#     #     for func in [
#     #         well_counts,
#     #         hull_outline,
#     #         epsg_codes,
#     #         dir_stats,
#     #         repo_mod,
#     #     ]:
#     #         md = func(repo_base)
#     #         repo_base.update(md)
#
#     return repo_list
#     # validated_repo_dicts = [validate_repo(r).to_dict() for r in repos]
#     # return validated_repo_dicts
