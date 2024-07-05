import uvicorn
from fastapi import FastAPI
from purr_geographix.api_modules import routes_setup
from assets.collect import routes_assets

app = FastAPI()

app.include_router(routes_setup.router, prefix="/purr/ggx")
app.include_router(routes_assets.router, prefix="/purr/ggx")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

# to run in console, comment out the uvicorn import and __name__ lines
# fastapi dev main.py   or   fastapi run main.py
