import asyncio
import uuid
from enum import Enum
from fastapi import APIRouter, HTTPException, status, Query, Path
from typing import Dict
from purr_geographix.assets.collect.handle_query import selector
from pydantic import BaseModel
from purr_geographix.core.database import get_db
from purr_geographix.core.crud import fetch_repo_ids
import purr_geographix.core.schemas as schemas
from purr_geographix.core.logger import logger


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


def parse_uwis(uwis: str | None) -> str | None:
    """Parse POSTed uwi string into a suitable SQLAnywhere SIMILAR TO clause.
    Split by commas or spaces, replace '*' with '%', joined to '|'

    Example:
        0505* pilot %0001 -> '0505%|pilot|%0001'

    Args:
        uwis (str): incoming request string

    Returns:
        str: A valid SIMILAR TO clause string
    """
    if uwis is None:
        return uwis

    try:
        cleaned = uwis.strip()
        split = [
            item for item in cleaned.replace(",", " ").replace('"', "").split() if item
        ]
        processed = [item.replace("*", "%").replace("'", "''") for item in split]
        uwi_query = "|".join(processed)
        logger.debug(f"parse_uwi returns: {uwi_query}")
        return uwi_query
    except AttributeError:
        logger.error(f"'uwis' must be a string, not {type(uwis)}")
        return uwis
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return uwis


router = APIRouter()

task_storage: Dict[str, schemas.AssetCollectionResponse] = {}


async def process_asset_collection(
        task_id: str, repo_id: str, asset: str, uwi_query: str
):
    try:
        task_storage[task_id].task_status = schemas.TaskStatus.IN_PROGRESS
        res = await selector(repo_id, asset, uwi_query)
        logger.info(res)
        task_storage[task_id].task_status = schemas.TaskStatus.COMPLETED
        return res
    except Exception as e:
        task_storage[task_id].task_status = schemas.TaskStatus.FAILED
        logger.error(f"Task failed for {task_id}: {str(e)}")


# ASSETS ######################################################################
@router.post(
    "/asset/{repo_id}/{asset}",
    response_model=schemas.AssetCollectionResponse,
    summary="Query a Repo for Asset data",
    description=(
            "Specify a repo_id, asset (data type) and an optional uwi filter. "
            "Query results will be written to files stored in the 'file_depot' "
            "directory."
    ),
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


@router.get(
    "/asset/status/{task_id}",
    response_model=schemas.AssetCollectionResponse,
    summary="Check status of a /asset/{repo_id}/{asset} job using the task_id.",
    description=(
            "An assect collection job may take several minutes, so use the task_id "
            "returned by the original POST to (periodically) check the job status. "
            "Status values are: pending, in_progress, completed or failed. Query "
            "results will be written to the file_depot directory."
    ),
)
async def get_asset_collect_status(task_id: str):
    if task_id not in task_storage:
        raise HTTPException(status_code=404, detail="Asset collection task not found")
    return task_storage[task_id]

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
