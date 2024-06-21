from sqlalchemy import Boolean, Column, Integer, String, UUID, JSON, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
import uuid

Base = declarative_base()


class Repo(Base):
    __tablename__ = "repos"

    # id = Column(UUID(as_uuid=True), primary_key=True, index=True)
    id = Column(String(64), primary_key=True, index=True, unique=True)
    active = Column(Boolean)
    name = Column(String)
    fs_path = Column(String)
    conn = Column(JSON)
    conn_aux = Column(JSON)
    suite = Column(String)
    well_count = Column(Integer)
    wells_with_completion = Column(Integer)
    wells_with_core = Column(Integer)
    wells_with_dst = Column(Integer)
    wells_with_formation = Column(Integer)
    wells_with_ip = Column(Integer)
    wells_with_perforation = Column(Integer)
    wells_with_production = Column(Integer)
    wells_with_raster_log = Column(Integer)
    wells_with_survey = Column(Integer)
    wells_with_vector_log = Column(Integer)
    wells_with_zone = Column(Integer)
    storage_epsg = Column(Integer)
    storage_name = Column(String)
    display_epsg = Column(Integer)
    display_name = Column(String)
    files = Column(Integer)
    directories = Column(Integer)
    bytes = Column(Integer)
    repo_mod = Column(TIMESTAMP)
    outline = Column(JSON)


class Setup(Base):
    __tablename__ = "setup"

    file_depot = Column(String, primary_key=True)
