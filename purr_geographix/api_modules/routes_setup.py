import asyncio
import json
import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Dict
import traceback
import purr_geographix.api_modules.schemas as schemas
import purr_geographix.api_modules.crud as crud
from purr_geographix.api_modules.database import get_db
from purr_geographix.common.util import is_valid_dir
from purr_geographix.recon.recon import repo_recon

router = APIRouter()

task_storage: Dict[str, schemas.RepoReconResponse] = {}


@router.post(
    "/setup/file_depot",
    response_model=schemas.Setup,
    summary="Storage for query result files.",
    description="TBD description",
)
def update_file_depot(file_depot: str, db: Session = Depends(get_db)):
    valid_dir = is_valid_dir(file_depot)
    if not valid_dir:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid directory: {file_depot}",
        )
    try:
        db_setup = crud.update_file_depot(db=db, file_depot=valid_dir)
        return db_setup
    except Exception as e:
        # Handle any database-related errors

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while updating the file_depot: {str(e)}",
        )


@router.get("/setup/", response_model=schemas.SetupWithRepos)
def read_setup(db: Session = Depends(get_db)):
    result = crud.get_setup(db)
    return schemas.SetupWithRepos(setup=result["setup"], repos=result["repos"])


# @router.get("/repos/", response_model=list[schemas.Repo])
# def read_repos(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     repos = crud.get_repos(db, skip=skip, limit=limit)
#     return repos


async def process_repo_recon(task_id: str, recon_root: str, ggx_host: str):
    try:
        task_storage[task_id].task_status = schemas.TaskStatus.IN_PROGRESS
        repos = await repo_recon(recon_root, ggx_host)
        print("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")
        for r in repos:
            print(json.dumps(r, indent=4))
        print("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")

        task_storage[task_id].task_status = schemas.TaskStatus.COMPLETED
    except Exception as e:
        task_storage[task_id].task_status = schemas.TaskStatus.FAILED
        print(f"Task failed for {task_id}: {str(e)}")
        stack_trace = traceback.format_exc()
        print(stack_trace)


@router.post(
    "/setup/repo_recon",
    response_model=schemas.RepoReconResponse,
    summary="Crawl for repos",
    description="Provide a top-level 'recon_root' path (i.e. Project Home) "
                "to scan for GeoGraphix project repos. Results are saved and used for future scans.",
    status_code=status.HTTP_202_ACCEPTED,
)
async def run_repo_recon(recon_root: str, ggx_host: str = "localhost"):
    valid_recon_root = is_valid_dir(recon_root)
    if not valid_recon_root:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid directory: {recon_root}",
        )
    task_id = str(uuid.uuid4())
    new_repo_recon = schemas.RepoReconResponse(
        id=task_id,
        recon_root=valid_recon_root,
        ggx_host=ggx_host,
        task_status=schemas.TaskStatus.PENDING,
    )
    task_storage[task_id] = new_repo_recon
    # do not await create_task, or it will block the response
    asyncio.create_task(process_repo_recon(task_id, valid_recon_root, ggx_host))
    return new_repo_recon


@router.get(
    "/setup/repo_recon/{task_id}",
    response_model=schemas.RepoReconResponse,
    summary="Check status of repo recon task",
    description="The repo_recon task may take several minutes. Use this to "
                "periodically check the job status.",
)
async def get_repo_recon_status(task_id: str):
    if task_id not in task_storage:
        raise HTTPException(status_code=404, detail="repo recon not found")
    return task_storage[task_id]
