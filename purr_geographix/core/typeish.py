from dataclasses import dataclass, field, asdict
from typing import List, Optional
from datetime import datetime


@dataclass
class SQLAnywhereConn:
    astart: str
    dbf: str
    dbn: str
    driver: str
    host: str
    pwd: str
    server: str
    uid: str
    port: Optional[int] = None

    def to_dict(self):
        return asdict(self)


# REPO ########################################################################


@dataclass
class ConnAux:
    ggx_host: str

    def to_dict(self):
        return asdict(self)


@dataclass
class Repo:
    id: str
    name: str
    fs_path: str
    conn: SQLAnywhereConn
    conn_aux: ConnAux
    suite: str
    well_count: int
    wells_with_completion: int
    wells_with_core: int
    wells_with_dst: int
    wells_with_formation: int
    wells_with_ip: int
    wells_with_perforation: int
    wells_with_production: int
    wells_with_raster_log: int
    wells_with_survey: int
    wells_with_vector_log: int
    wells_with_zone: int
    storage_epsg: int
    storage_name: str
    display_epsg: int
    display_name: str
    files: int
    directories: int
    bytes: int
    repo_mod: str
    polygon: List[List[float]] = field(default_factory=list)
    active: Optional[bool] = True

    def to_dict(self):
        repo_dict = asdict(self)
        repo_dict["conn"] = self.conn.to_dict()
        repo_dict["conn_aux"] = self.conn_aux.to_dict()
        repo_dict["repo_mod"] = datetime.strptime(self.repo_mod, "%Y-%m-%d %H:%M:%S")
        return repo_dict


# #############################################################################


def validate_repo(payload: dict):
    # remove supabase audit columns
    # unwanted_keys = ["active", "created_at", "touched_at", "updated_at"]
    # for key in unwanted_keys:
    #     if key in payload:
    #         payload.pop(key)

    return Repo(
        id=payload["id"],
        active=payload["active"],
        name=payload["name"],
        fs_path=payload["fs_path"],
        conn=SQLAnywhereConn(**payload["conn"]),
        conn_aux=ConnAux(**payload["conn_aux"]),
        suite=payload["suite"],
        well_count=payload["well_count"],
        wells_with_completion=payload["wells_with_completion"],
        wells_with_core=payload["wells_with_core"],
        wells_with_dst=payload["wells_with_dst"],
        wells_with_formation=payload["wells_with_formation"],
        wells_with_ip=payload["wells_with_ip"],
        wells_with_perforation=payload["wells_with_perforation"],
        wells_with_production=payload["wells_with_production"],
        wells_with_raster_log=payload["wells_with_raster_log"],
        wells_with_survey=payload["wells_with_survey"],
        wells_with_vector_log=payload["wells_with_vector_log"],
        wells_with_zone=payload["wells_with_zone"],
        storage_epsg=payload["storage_epsg"],
        storage_name=payload["storage_name"],
        display_epsg=payload["display_epsg"],
        display_name=payload["display_name"],
        files=payload["files"],
        directories=payload["directories"],
        bytes=payload["bytes"],
        repo_mod=payload["repo_mod"],
        polygon=payload["polygon"],
    )
