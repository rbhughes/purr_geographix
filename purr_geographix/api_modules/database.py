from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# from .models import models_a
import purr_geographix.api_modules.models as models


SQLALCHEMY_DATABASE_URL = "sqlite:///./purrio.sqlite"
# SQLALCHEMY_DATABASE_URL = "postgresql://user:password@postgresserver/db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


models.Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
