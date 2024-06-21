from pydantic import BaseModel
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime


class SetupBase(BaseModel):
    file_depot: str


class Setup(SetupBase):
    class Config:
        from_attributes = True


# class Coordinates(BaseModel):
#     coordinates: List[Tuple[float, float]] = Field(
#         ...,
#         description="A list of longitude-latitude pairs",
#         example=[
#             [-97.90777, 27.62867],
#             [-97.91186, 27.62672]
#         ]
#     )


class RepoBase(BaseModel):
    active: bool
    name: str
    fs_path: str
    conn: Dict[str, Any]
    conn_aux: Dict[str, Any] | None
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
    repo_mod: datetime
    polygon: List[Tuple[float, float]] | None


class Repo(RepoBase):
    # id: UUID
    id: str

    class Config:
        from_attributes = True


class SetupWithRepos(BaseModel):
    setup: Setup
    repos: List[Repo]


# class Setup(BaseModel):
#     file_depot: str
#
#
# class SetupCreate(Setup):
#     pass
#


class TaskStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class RepoReconCreate(BaseModel):
    recon_root: str
    ggx_host: Optional[str] = None


class RepoReconResponse(BaseModel):
    id: str
    recon_root: str
    ggx_host: str
    task_status: TaskStatus
