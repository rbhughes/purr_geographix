import tempfile
from typing import Dict, Union, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import insert, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import purr_geographix.core.models as models
from purr_geographix.core.logger import logger


def get_settings(db: Session) -> Dict[str, Union[models.Settings, List[models.Repo]]]:
    settings = db.query(models.Settings).first()
    repos = db.query(models.Repo).all()
    return {"settings": settings, "repos": repos}


def get_file_depot(db: Session) -> Optional[str]:
    setting = db.query(models.Settings.file_depot).first()
    return setting.file_depot


def init_file_depot(db: Session) -> None:
    file_depot = db.query(models.Settings.file_depot).first()
    if file_depot is None:
        temp_loc = tempfile.gettempdir()
        logger.info("(Re)initializing file_depot to:", temp_loc)
        stmt = insert(models.Settings).values(file_depot=temp_loc)
        db.execute(stmt)
        db.commit()


def update_file_depot(db: Session, file_depot: str) -> Optional[str]:
    # not an upsert since there is no independent id key
    fd_count = db.query(models.Settings.file_depot).count()
    if fd_count == 0:
        stmt = insert(models.Settings).values(file_depot=file_depot)
    else:
        stmt = update(models.Settings).values(file_depot=file_depot)
    db.execute(stmt)
    db.commit()
    setting = db.query(models.Settings.file_depot).first()
    return setting.file_depot


def upsert_repos(db: Session, repos: List[models.Repo]) -> List[models.Repo]:
    logger.info(f"Upserting {len(repos)} repos")
    stmt = sqlite_insert(models.Repo).values(repos)
    update_dict = {c.name: c for c in stmt.excluded if c.name != "id"}
    stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=update_dict)
    db.execute(stmt, repos)
    db.commit()
    ids = [repo["id"] for repo in repos]
    updated_repos = db.query(models.Repo).filter(models.Repo.id.in_(ids)).all()
    return updated_repos


def get_repos(db: Session) -> List[models.Repo]:
    repos = db.query(models.Repo).all()
    return repos


def get_repo_by_id(db: Session, repo_id: str) -> models.Repo:
    repo = db.query(models.Repo).filter_by(id=repo_id).first()
    return repo


def fetch_repo_ids(db: Session) -> List[str]:
    repo_ids = db.query(models.Repo.id).all()
    return [repo_id[0] for repo_id in repo_ids]
