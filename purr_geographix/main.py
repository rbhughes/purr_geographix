"""Main entry point of purr_geographix"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn
from purr_geographix.core import routes_settings
from purr_geographix.assets.collect import routes_assets
from purr_geographix.core.crud import init_file_depot
from purr_geographix.core.database import get_db
from purr_geographix.core.logger import logger


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

if __name__ == "__main__":
    logger.info("Initializing purr_geographix API")
    uvicorn.run(app, host="0.0.0.0", port=8000)

# to run in console, comment out the uvicorn import and __name__ lines
# fastapi dev main.py   or   fastapi run main.py
