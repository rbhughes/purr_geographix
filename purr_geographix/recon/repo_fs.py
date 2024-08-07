"""Stuff involving metadata about a Repo filesystem"""

import asyncio
import os
import re
from datetime import datetime
from pathlib import Path
from subprocess import run
from typing import List
from purr_geographix.core.logger import logger


def looks_like_ggx_project(directory: str) -> bool:
    """Check if a directory looks like a GeoGraphix project

    Args:
        directory (str): The directory path to check.

    Returns:
        bool: True if the directory appears to be a GeoGraphix project.
    """
    dir_path = Path(directory)

    gxdb_file = dir_path / "gxdb.db"
    gxdb_prod_file = dir_path / "gxdb_production.db"
    global_aoi_dir = dir_path / "Global"

    return all(
        [
            gxdb_file.is_file(),
            gxdb_prod_file.is_file(),
            global_aoi_dir.is_dir(),
        ]
    )


async def walk_dir_for_gxdb(path: str) -> List[str]:
    """A coroutine used to recursively crawl a directory for GGX-like paths

    Using os.walk was about ~20% faster than dir.rglob.

    Args:
        path (str): The root directory path to start the crawl.

    Returns:
        List[str]: A list of directories that appear to be GeoGraphix projects.
    """
    root_dir = Path(path)
    potential_repos = []

    for root, _, files in os.walk(root_dir):
        if any(file.endswith("gxdb.db") for file in files):
            if looks_like_ggx_project(root):
                potential_repos.append(root)

    return potential_repos


async def network_repo_scan(recon_root: str) -> List[str]:
    """Scan the network to locate GeoGraphix projects.

    Using asyncio.gather is simpler and performed comparable to dask.bag

    Args:
        recon_root (str): The root directory path to start the scan.

    Returns:
        List[str]: A list of GeoGraphix projects (repos).
    """

    root_dir = Path(recon_root)
    top_dirs = [str(path) for path in root_dir.iterdir() if path.is_dir()]

    if looks_like_ggx_project(recon_root):
        repos = [recon_root]
    else:
        repos = []

    coroutines = [walk_dir_for_gxdb(path) for path in top_dirs]
    all_repos = await asyncio.gather(*coroutines)

    # flattens list of lists, removes duplicates and returns unique repos
    repos.extend([repo for sublist in all_repos for repo in sublist])
    return list(set(repos))


def dir_stats(repo_base) -> dict:
    """Use microsoft's du utility to collect directory size.
    It's faster than vanilla python.
    https://learn.microsoft.com/en-us/sysinternals/downloads/du

    Args:
        repo_base (dict): A stub repo dict. We just use the fs_path

    Returns:
        dict of parsed stdout metadata
    """
    logger.info(f"dir_stats: {repo_base['fs_path']}")

    base_dir = Path(__file__).resolve().parent.parent
    du64 = base_dir / "bin" / "du64.exe"

    res = run(
        [du64, "-q", "-nobanner", repo_base["fs_path"]],
        capture_output=True,
        text=True,
        check=False,
    )
    meta = {}
    lines = res.stdout.splitlines()
    for line in lines:
        if line:
            pair = line.split(":")
            left = pair[0].strip()
            right = pair[1].replace("bytes", "").replace(",", "").strip()
            if left == "Size":
                meta["bytes"] = int(right)
            elif left != "Size on disk":
                meta[left.lower()] = int(right)
    return meta


def repo_mod(repo_base) -> dict:
    """Walk the project directory to get the latest file modification date.

    We exclude SQLAnywhere files since they are modified simply by making
    an ODBC connection.

    TODO: Needs serious optimization, maybe multi-thread like os.walk?

    Args:
        repo_base (dict): A stub repo dict. We just use the fs_path

    Returns:
        dict: with repo_mod as datetime string
    """
    logger.info(f"repo_mod: {repo_base['fs_path']}")

    last_mod = datetime(1970, 1, 1)
    gxdb_matcher = re.compile(r"gxdb\.db$|gxdb_production\.db$|gxdb\.log$")

    for root, _, files in os.walk(repo_base["fs_path"]):
        for file in files:
            if not gxdb_matcher.match(file):
                full_path = os.path.join(root, file)
                try:
                    stat = os.stat(full_path)
                    mod_time = datetime.fromtimestamp(stat.st_mtime)
                    if mod_time > last_mod:
                        last_mod = mod_time
                except (OSError, ValueError):
                    continue

    return {"repo_mod": last_mod.strftime("%Y-%m-%d %H:%M:%S")}
