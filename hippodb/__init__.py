__version__ = "0.1.0"

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from . import api

app = FastAPI(title="HippoDB", version=__version__, lifespan=api.hippo_lifespan)
app.include_router(api.router)
app.include_router(api.application)

templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def root():
    return """<html lang="en">
    <head>
        <title>Home — HippoDB</title>
    </head>
    <body>Hello, world!</body>
</html>"""
