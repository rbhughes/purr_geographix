import asyncio

# import json
import uuid
from enum import Enum
from fastapi import APIRouter, HTTPException, status, Query, Path
from typing import Dict
from purr_geographix.assets.collect.handle_query import selector
from pydantic import BaseModel
from purr_geographix.core.database import get_db
from purr_geographix.core.crud import fetch_repo_ids
import purr_geographix.core.schemas as schemas


# from fastapi.responses import JSONResponse
# from purr_geographix.common.util import CustomEncoder


# class CustomJSONResponse(JSONResponse):
#     def render(self, content: any) -> bytes:
#         return json.dumps(
#             content,
#             ensure_ascii=False,
#             allow_nan=False,
#             indent=None,
#             separators=(",", ":"),
#             cls=CustomEncoder,
#         ).encode("utf-8")


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


def parse_uwis(uwis: str) -> str:
    if uwis is None:
        return uwis

    try:
        cleaned = uwis.strip()
        split = [
            item for item in cleaned.replace(",", " ").replace('"', "").split() if item
        ]
        processed = [item.replace("*", "%").replace("'", "''") for item in split]
        return "|".join(processed)
    except Exception as e:
        return uwis


router = APIRouter()

task_storage: Dict[str, schemas.AssetCollectionResponse] = {}


async def process_asset_collection(
    task_id: str, repo_id: str, asset: str, uwi_query: str
):
    try:
        task_storage[task_id].task_status = schemas.TaskStatus.IN_PROGRESS
        res = await selector(repo_id, asset, uwi_query)
        print("AAAAAAAAAAAAAAAAAAAAAAAA")
        print(res)
        print("AAAAAAAAAAAAAAAAAAAAAAAA")
        task_storage[task_id].task_status = schemas.TaskStatus.COMPLETED
        return res
    except Exception as e:
        task_storage[task_id].task_status = schemas.TaskStatus.FAILED
        print(f"Task failed for {task_id}: {str(e)}")


@router.get(
    "/asset/status/{task_id}",
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
    "/asset/{repo_id}/{asset}",
    response_model=schemas.AssetCollectionResponse,
    summary="get asset data",
    description="some kinda description",
    status_code=status.HTTP_202_ACCEPTED,
)
async def asset_collection(
    repo_id: str = Path(..., description="repo_id"),
    asset: AssetTypeEnum = Path(..., description="asset type"),
    uwi_query: str = Query(
        None,
        min_length=3,
        description="Enter full or partial uwi(s); use * or % as wildcard."
        "Separate UWIs with spaces or commas. Leave blank to select all.",
    ),
):
    RepoId.validate_repo_id(repo_id)
    asset = asset.value

    uwi_query = parse_uwis(uwi_query)

    task_id = str(uuid.uuid4())
    new_collect = schemas.AssetCollectionResponse(
        id=task_id,
        repo_id=repo_id,
        asset=asset,
        uwi_query=uwi_query,
        task_status=schemas.TaskStatus.PENDING,
    )
    task_storage[task_id] = new_collect

    # noinspection PyAsyncCall
    asyncio.create_task(
        process_asset_collection(
            task_id,
            repo_id,
            asset,
            uwi_query,
        )
    )
    return new_collect


# @router.post(
#     "/collect_data/{repo_id}/{asset}",
#     summary="get asset data",
#     description="some kinda description",
# )
# async def asset_collection(
#     repo_id: str = Path(..., description="repo_id"),
#     asset: AssetTypeEnum = Path(..., description="asset type"),
#     uwi_query: str = Query(
#         None,
#         min_length=3,
#         description="Enter full or partial uwi(s); use * or % as wildcard."
#         "Separate UWIs with spaces or commas. Leave blank to select all.",
#     ),
# ):
#     RepoId.validate_repo_id(repo_id)
#     asset = asset.value
#
#     uwi_query = parse_uwis(uwi_query)
#
#     task_id = str(uuid.uuid4())
#     new_collect = schemas.AssetCollectionResponse(
#         id=task_id,
#         repo_id=repo_id,
#         asset=asset,
#         uwi_query=uwi_query,
#         task_status=schemas.TaskStatus.PENDING,
#     )
#     task_storage[task_id] = new_collect
#
#     result = await asyncio.create_task(
#         process_asset_collection(
#             task_id,
#             repo_id,
#             asset,
#             uwi_query,
#         )
#     )
#
#     return CustomJSONResponse(content=result)
