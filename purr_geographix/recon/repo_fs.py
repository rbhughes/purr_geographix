import asyncio
import os
import re
from datetime import datetime
from pathlib import Path
from subprocess import run
from typing import List


def is_ggx_project(directory: str) -> bool:
    """
    Determines if a directory looks like a GeoGraphix project by checking for
    SQLAnywhere database files and a Global AOI directory.

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
    """
    Recursively crawl a directory and return a list of directory paths that
    appear to be GeoGraphix projects.
    (os.walk was about ~20% faster than dir.rglob. YMMV)

    Args:
        path (str): The root directory path to start the crawl.

    Returns:
        List[str]: A list of directories that appear to be GeoGraphix projects.
    """
    root_dir = Path(path)
    potential_repos = []

    for root, dirs, files in os.walk(root_dir):
        if any(file.endswith("gxdb.db") for file in files):
            if is_ggx_project(root):
                potential_repos.append(root)

    return potential_repos


async def network_repo_scan(recon_root: str) -> List[str]:
    """
    Scan the network to locate GeoGraphix projects.
    (asyncio.gather is simpler has roughly the same performance as dask.bag)

    Args:
        recon_root (str): The root directory path to start the scan.

    Returns:
        List[str]: A list of GeoGraphix projects (repos).
    """

    root_dir = Path(recon_root)
    top_dirs = [str(path) for path in root_dir.iterdir() if path.is_dir()]

    if is_ggx_project(recon_root):
        repos = [recon_root]
    else:
        repos = []

    coroutines = [walk_dir_for_gxdb(path) for path in top_dirs]
    all_repos = await asyncio.gather(*coroutines)

    # flattens list of lists, removes duplicates and returns unique repos
    repos.extend([repo for sublist in all_repos for repo in sublist])
    return list(set(repos))

    # root_dir = Path(recon_root)
    # top_dirs = [str(path) for path in root_dir.iterdir() if path.is_dir()]
    #
    # directories = db.from_sequence(top_dirs, npartitions=10)
    # # num_parallel_tasks = directories.npartitions
    # # print(f"Using {num_parallel_tasks} parallel tasks")
    # all_repos = directories.map(walk_dir_for_gxdb).flatten().compute()
    # repos = list(set(all_repos))
    # return repos


def dir_stats(repo_base) -> dict:
    """
    https://learn.microsoft.com/en-us/sysinternals/downloads/du
    Run microsoft's du utility to collect directory size. Faster than python.
    :param repo_base: A stub repo dict. We just use the fs_path
    :return: dict of parsed stdout byte sizes
    """
    base_dir = Path(__file__).resolve().parent.parent
    du64 = base_dir / 'bin' / 'du64.exe'

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

# async def main():
#     import time
#
#     t0 = time.time()
#     recon_root = r"d:/"
#
#     repos = await network_repo_scan(recon_root)
#     t1 = time.time()
#
#     for r in repos:
#         print(r)
#     print(t1 - t0)
#
#
# if __name__ == "__main__":
#     asyncio.run(main())
