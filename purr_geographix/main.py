"""Main entry point of purr_geographix"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
import os
import uvicorn
from purr_geographix.core import routes_settings
from purr_geographix.assets.collect import routes_assets
from purr_geographix.core.crud import init_file_depot
from purr_geographix.core.database import get_db
from purr_geographix.core.logger import logger
from purr_geographix.prep.setup import prepare


@asynccontextmanager
async def lifespan(fastapp: FastAPI):  # pylint: disable=unused-argument
    """https://fastapi.tiangolo.com/advanced/events/

    Args:
        fastapi (FastAPI): FastAPI instance, not actually used in the function
    """
    db = next(get_db())
    init_file_depot(db)
    yield


app = FastAPI(lifespan=lifespan)

app.include_router(routes_settings.router, prefix="/purr/ggx")
app.include_router(routes_assets.router, prefix="/purr/ggx")

# to generate current openapi schema
# import json
# openapi_schema = app.openapi()
# with open("./docs/openapi.json", "w") as f:
#     json.dump(openapi_schema, f, indent=2)

purr_port = int(os.environ.get("PURR_GEOGRAPHIX_PORT", "8060"))
purr_host = os.environ.get("PURR_GEOGRAPHIX_HOST", "0.0.0.0")
purr_workers = int(os.environ.get("PURR_GEOGRAPHIX_WORKERS", "4"))


def prep():
    logger.info("Installing Sysinternals DU utility")
    prepare()


def start():
    logger.info("Initializing purr_geographix API (prod)")
    uvicorn.run(
        "purr_geographix.main:app",
        host=purr_host,
        port=purr_port,
        workers=purr_workers,
    )


if __name__ == "__main__":
    logger.info("Initializing purr_geographix API (dev)")
    uvicorn.run(app, host=purr_host, port=purr_port)

# to run in dev, just do:   python -m purr_geographix.main
