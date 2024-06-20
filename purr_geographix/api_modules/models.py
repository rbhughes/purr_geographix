from sqlalchemy import Boolean, Column, Integer, String, UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Repo(Base):
    __tablename__ = "repos"

    id = Column(UUID, primary_key=True, index=True)
    active = Column(Boolean, index=False)
    name = Column(String, index=False)
    fs_path = Column(String, index=False)
    well_count = Column(Integer, index=False)


class Setup(Base):
    __tablename__ = "setup"

    file_depot = Column(String, primary_key=True)
