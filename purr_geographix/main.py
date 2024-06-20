from fastapi import FastAPI

# from flounder.routes import routes_a, routes_b, routes_c
from purr_geographix.api_modules import routes_setup

import uvicorn


app = FastAPI()

app.include_router(routes_setup.router, prefix="/purr/v1")

# app.include_router(routes_a.router, prefix="/aaa/v1")
# app.include_router(routes_b.router, prefix="/bbb/v1")
# app.include_router(routes_c.router, prefix="/ccc/v1")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

# to run in console, comment out the uvicorn import and __name__ lines
# fastapi dev main.py   or   fastapi run main.py
