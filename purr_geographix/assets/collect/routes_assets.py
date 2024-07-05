import asyncio
import uuid
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path
from typing import Annotated, Dict, List, Union, Type

import purr_geographix.api_modules.schemas as schemas
from purr_geographix.assets.collect.handle_query import selector
from pydantic import BaseModel
from purr_geographix.api_modules.database import get_db
from enum import Enum

from purr_geographix.api_modules.crud import fetch_repo_ids


class RepoId(BaseModel):
    repo_id: str

    @classmethod
    def validate_repo_id(cls, repo_id):
        db = next(get_db())
        valid_repo_ids = fetch_repo_ids(db)
        if repo_id not in valid_repo_ids:
            raise HTTPException(status_code=400, detail=f"Invalid repo_id: {repo_id}")
        return repo_id


class AssetTypeEnum(str, Enum):
    completion = "completion"
    core = "core"
    dst = "dst"
    formation = "formation"
    ip = "ip"
    perforation = "perforation"
    production = "production"
    raster_log = "raster_log"
    survey = "survey"
    vector_log = "vector_log"
    well = "well"
    zone = "zone"


router = APIRouter()

task_storage: Dict[str, schemas.AssetCollectionResponse] = {}


async def process_asset_collection(
        task_id: str, repo_id: str, asset: str, uwi_query: str
):
    try:
        task_storage[task_id].task_status = schemas.TaskStatus.IN_PROGRESS
        res = await selector(repo_id, asset, uwi_query)
        # print("........")
        # print(res)
        # print("........")
        print("ASSET THING DONE")

        task_storage[task_id].task_status = schemas.TaskStatus.COMPLETED
    except Exception as e:
        task_storage[task_id].task_status = schemas.TaskStatus.FAILED
        print(f"Task failed for {task_id}: {str(e)}")


@router.get(
    "/collect/{task_id}",
    response_model=schemas.AssetCollectionResponse,
    summary="Check asset collection status",
    description="The asset collection task may take several minutes. Use this "
                "to periodically check the job status.",
)
async def get_asset_collect_status(task_id: str):
    if task_id not in task_storage:
        raise HTTPException(status_code=404, detail="Asset collection task not found")
    return task_storage[task_id]


@router.post(
    "/collect/{repo_id}/{asset}",
    response_model=schemas.AssetCollectionResponse,
    summary="get asset data",
    description="some kinda description",
    status_code=status.HTTP_202_ACCEPTED,
)
async def asset_collection(
        repo_id: str = Path(..., description="repo_id"),
        asset: AssetTypeEnum = Path(..., description="asset type"),
        uwi_query: str = Query(
            None, min_length=3, description="Enter UWI (or partial UWI)"
        ),
        # uwi_query: str = Query(
        #     ..., min_length=3, max_length=50, description="The search query"
        # ),
):
    RepoId.validate_repo_id(repo_id)
    asset = asset.value

    task_id = str(uuid.uuid4())
    new_collect = schemas.AssetCollectionResponse(
        id=task_id,
        repo_id=repo_id,
        asset=asset,
        uwi_query=uwi_query,
        task_status=schemas.TaskStatus.PENDING,
    )
    print("................")
    print(new_collect)
    print("................")
    task_storage[task_id] = new_collect
    # no await!
    asyncio.create_task(process_asset_collection(task_id, repo_id, asset, uwi_query))
    return new_collect