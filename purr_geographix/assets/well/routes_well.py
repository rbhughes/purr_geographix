import asyncio
import json
import uuid
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path
from sqlalchemy.orm import Session
from typing import Dict, List, Union
import purr_geographix.api_modules.schemas as schemas
from purr_geographix.assets.well.well import sayit, selector

router = APIRouter()

task_storage: Dict[str, schemas.WellThingResponse] = {}


# @router.get("/repos/", response_model=list[schemas.Repo])
# def read_repos(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     repos = crud.get_repos(db, skip=skip, limit=limit)
#     return repos


def parse_repo_ids(
        repo_ids: str = Path(
            ...,
            description="Single repository ID or comma-separated list of repository IDs",
        )
) -> Union[str, List[str]]:
    if "," in repo_ids:
        # Multiple repo IDs
        id_list = [
            repo_id.strip() for repo_id in repo_ids.split(",") if repo_id.strip()
        ]
        if not id_list:
            raise ValueError("No valid repository IDs provided")
        return id_list
    else:
        # Single repo ID
        if not repo_ids.strip():
            raise ValueError("Invalid repository ID")
        return repo_ids.strip()


async def process_well_thing(task_id: str, repo_ids: List[str], query: str):
    try:
        task_storage[task_id].task_status = schemas.TaskStatus.IN_PROGRESS
        # repos = await repo_recon(recon_root, ggx_host)
        # print("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")
        # for r in repos:
        #     print(json.dumps(r, indent=4))
        # print("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")

        # await asyncio.sleep(3)
        res = await selector(repo_ids, query)
        print("........")
        # print("........")
        # print(task_storage)
        # sayit(repo_ids)
        # print(repo_ids)
        # print(query)
        # print("........")
        print("WELLTHING DONE")

        task_storage[task_id].task_status = schemas.TaskStatus.COMPLETED
    except Exception as e:
        task_storage[task_id].task_status = schemas.TaskStatus.FAILED
        print(f"Task failed for {task_id}: {str(e)}")


@router.get(
    "/well/{task_id}",
    response_model=schemas.WellThingResponse,
    summary="Check status of wellthing",
    description="The wellthing task may take several minutes. Use this to "
                "periodically check the job status.",
)
async def get_well_thing_status(task_id: str):
    if task_id not in task_storage:
        raise HTTPException(status_code=404, detail="repo recon not found")
    return task_storage[task_id]


@router.post("/well/{repo_ids}/")
async def search(
        repo_ids: Union[str, List[str]] = Depends(parse_repo_ids),
        query: str = Query(
            ..., min_length=3, max_length=50, description="The search query"
        ),
        # page: int = Query(1, ge=1, description="Page number"),
        # limit: int = Query(10, ge=1, le=100, description="Number of results per page"),
):
    print("do some well thing")
    task_id = str(uuid.uuid4())
    new_well_thing = schemas.WellThingResponse(
        id=task_id,
        repo_ids=repo_ids,
        query=query,
        task_status=schemas.TaskStatus.PENDING,
    )
    task_storage[task_id] = new_well_thing
    asyncio.create_task(process_well_thing(task_id, repo_ids, query))
    return new_well_thing

    # return {"repo_ids": repo_ids, "query": query, "page": page, "limit": limit}
