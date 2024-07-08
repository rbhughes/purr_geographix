from sqlalchemy.orm import Session
from sqlalchemy import insert, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import tempfile
from typing import List
import purr_geographix.core.models as models


def get_settings(db: Session):
    settings = db.query(models.Settings).first()
    repos = db.query(models.Repo).all()
    return {"settings": settings, "repos": repos}


def get_file_depot(db: Session):
    settings = db.query(models.Settings).first()
    result = db.query(models.Settings.file_depot).first()
    return result


def init_file_depot(db: Session):
    file_depot = db.query(models.Settings.file_depot).first()
    if file_depot is None:
        temp_loc = tempfile.gettempdir()
        print("(Re)initializing file_depot to:", temp_loc)
        stmt = insert(models.Settings).values(file_depot=temp_loc)
        db.execute(stmt)
        db.commit()


def update_file_depot(db: Session, file_depot: str):
    # we do not do an upsert since there is no independent id key
    fd_count = db.query(models.Settings.file_depot).count()
    if fd_count == 0:
        stmt = insert(models.Settings).values(file_depot=file_depot)
    else:
        stmt = update(models.Settings).values(file_depot=file_depot)
    db.execute(stmt)
    db.commit()
    result = db.query(models.Settings.file_depot).first()
    return result


def upsert_repos(db: Session, repos: List[models.Repo]):
    stmt = sqlite_insert(models.Repo).values(repos)
    update_dict = {c.name: c for c in stmt.excluded if c.name != "id"}
    stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=update_dict)
    db.execute(stmt, repos)
    db.commit()
    ids = [repo["id"] for repo in repos]
    updated_repos = db.query(models.Repo).filter(models.Repo.id.in_(ids)).all()
    return updated_repos


def get_repos(db: Session):
    repos = db.query(models.Repo).all()
    return repos


def get_repo_by_id(db: Session, repo_id: str):
    repo = db.query(models.Repo).filter_by(id=repo_id).first()
    return repo


def fetch_repo_ids(db: Session) -> List[str]:
    repo_ids = db.query(models.Repo.id).all()
    return [repo_id[0] for repo_id in repo_ids]
