import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from purr_geographix.core import routes_settings
from purr_geographix.assets.collect import routes_assets
from purr_geographix.core.crud import init_file_depot
from purr_geographix.core.database import get_db
from purr_geographix.core.logger import logger


# noinspection PyShadowingNames
@asynccontextmanager
async def lifespan(app: FastAPI):
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
    host = "0.0.0.0"
    port = 8000
    logger.info(f"Initializing purr_geographix API. host={host} port={port}")
    uvicorn.run(app, host=host, port=port)

# to run in console, comment out the uvicorn import and __name__ lines
# fastapi dev main.py   or   fastapi run main.py
