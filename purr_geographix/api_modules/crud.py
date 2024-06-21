from sqlalchemy.orm import Session
from fastapi import Depends
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import text
from typing import List
# import uuid
from purr_geographix.api_modules.database import get_db


import purr_geographix.api_modules.schemas as schemas
import purr_geographix.api_modules.models as models


def get_setup(db: Session):
    setup = db.query(models.Setup).first()
    repos = db.query(models.Repo).all()
    return {"setup": setup, "repos": repos}


# THIS WORKS as of 2024-06-16
# def upsert_settings(db: Session, settings: schemas.SettingsCreate):
#     stmt = text("""
#         INSERT INTO settings (file_depot)
#         VALUES (:file_depot)
#         ON CONFLICT(file_depot) DO UPDATE SET file_depot = excluded.file_depot
#         """)
#     db.execute(stmt, {"file_depot": settings.file_depot})
#     db.commit()
#
#     result = db.execute(text("SELECT file_depot FROM settings")).fetchone()
#     return schemas.Settings(file_depot=result[0] if result else None)

# works for settings everything at once
# def write_settings(db: Session, settings: schemas.SettingsCreate):
#     delete_stmt = text("DELETE FROM settings")
#     db.execute(delete_stmt)
#     insert_stmt = text("INSERT INTO settings (file_depot) VALUES (:file_depot)")
#     db.execute(insert_stmt, {"file_depot": settings.file_depot})
#     db.commit()
#     result = db.execute(text("SELECT file_depot FROM settings")).fetchone()
#     return schemas.Settings(file_depot=result[0] if result else None)


def update_file_depot(db: Session, file_depot: str):
    existing_file_depot = db.execute(text("SELECT COUNT(*) FROM setup")).scalar()

    if existing_file_depot == 0:
        insert_stmt = text("INSERT INTO setup (file_depot) VALUES (:file_depot)")
        db.execute(insert_stmt, {"file_depot": file_depot})
    else:
        update_stmt = text("UPDATE setup SET file_depot = :file_depot")
        db.execute(update_stmt, {"file_depot": file_depot})
    db.commit()
    result = db.execute(text("SELECT file_depot FROM setup")).fetchone()
    return schemas.Setup(file_depot=result[0] if result else None)


# def get_repos(db: Session, skip: int = 0, limit: int = 100):
#     return db.query(models.Repo).offset(skip).limit(limit).all()


def upsert_repos(db, repos: List[models.Repo]):
    stmt = insert(models.Repo).values(repos)

    update_dict = {c.name: c for c in stmt.excluded if c.name != 'id'}

    stmt = stmt.on_conflict_do_update(
        index_elements=['id'],
        set_=update_dict
    )
    db.execute(stmt, repos)
    db.commit()

    ids = [repo['id'] for repo in repos]
    updated_repos = db.query(models.Repo).filter(models.Repo.id.in_(ids)).all()

    return updated_repos

