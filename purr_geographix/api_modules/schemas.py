from pydantic import BaseModel
from uuid import UUID
from enum import Enum
from typing import List, Optional


class SetupBase(BaseModel):
    file_depot: str


class Setup(SetupBase):
    class Config:
        from_attributes = True


class RepoBase(BaseModel):
    active: bool
    name: str
    fs_path: str
    well_count: int | None = None


class Repo(RepoBase):
    id: UUID

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
#


###


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
