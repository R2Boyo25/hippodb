from contextlib import asynccontextmanager
from http.client import FORBIDDEN, UNAUTHORIZED
from typing import Any
from uuid import uuid4
from urllib.parse import unquote
from typing_extensions import Annotated, TypedDict, cast
import orjson
from pydantic import BaseModel
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from hippodb.hippo import HippoDB, Token
from . import __version__


HIPPODB = cast(HippoDB, None)


@asynccontextmanager
async def hippo_lifespan(_app: FastAPI):
    global HIPPODB
    HIPPODB = HippoDB()

    yield

    HIPPODB.cleanup()
    HIPPODB = cast(HippoDB, None)


router = APIRouter(prefix="/api")


ServerInfo = TypedDict(
    "ServerInfo", {"version": str, "features": list[str], "vendor": dict[str, str]}
)


@router.get(
    "/",
)
def server_info() -> ServerInfo:
    return {
        "version": __version__,
        "features": [],
        "vendor": {
            "name": "Ohin Taylor <kazani@kazani.dev>",
        },
    }


class ApplicationView:
    def __init__(self, token: Token):
        self.app_id = token.application
        self.writable = token.writeable


http_security = HTTPBasic()


def application_dependency(
    credentials: Annotated[HTTPBasicCredentials, Depends(http_security)]
):
    if credentials.username not in HIPPODB.applications.keys():
        raise HTTPException(UNAUTHORIZED, "Application does not exist.")

    if credentials.password not in HIPPODB.tokens.keys():
        raise HTTPException(UNAUTHORIZED, "Invalid token.")

    if HIPPODB.tokens[credentials.password].application != credentials.username:
        raise HTTPException(UNAUTHORIZED, "Token and application do not match.")

    return ApplicationView(HIPPODB.tokens[credentials.password])


ApplicationDependency = Annotated[ApplicationView, Depends(application_dependency)]

application = APIRouter(prefix="/api")


class ApplicationInfo(BaseModel):
    id: str
    name: str


@application.get("/apps", tags=["application"])
def list_apps() -> list[ApplicationInfo]:
    return [
        ApplicationInfo(id=app.id, name=app.name)
        for app in HIPPODB.applications.values()
    ]


@application.post("/apps/new", tags=["application"])
def new_application(name: str) -> ApplicationInfo:
    app = HIPPODB.create_application(name)

    return ApplicationInfo(id=app.id, name=app.name)


@application.delete("/apps/delete", tags=["application"])
def delete_application(app_id: str, app_view: ApplicationDependency) -> None:
    if app_view.app_id != app_id:
        raise HTTPException(
            FORBIDDEN, "You do not have permission to delete this application."
        )

    HIPPODB.delete_application(app_id)


@application.post("/tokens/new", tags=["token"])
def new_token(app_id: str, writeable: bool = False) -> str:
    return HIPPODB.create_token(app_id, writeable).id


@application.delete("/tokens/delete", tags=["token"])
def delete_token(token_id: str, app_view: ApplicationDependency) -> None:
    if app_view.app_id != HIPPODB.tokens[token_id].application:
        raise HTTPException(
            FORBIDDEN, "You do not have permission to delete this token."
        )

    HIPPODB.delete_token(token_id)


### Databases ###


class DatabaseInfo(BaseModel):
    path: str

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"path": "/"},
            ],
        },
    }


def process_db_name(name: str) -> str:
    name = unquote(name)

    if not name.startswith("/"):
        name = "/" + name

    return name


@application.post("/create_db", tags=["database"])
def new_database(app_view: ApplicationDependency, path: str) -> DatabaseInfo:
    return DatabaseInfo(path=HIPPODB.create_database(app_view.app_id, path).path)


@application.get("/dbs/{db_name}", tags=["database"])
def list_databases(
    app_view: ApplicationDependency, db_name: str = "/", recursive: bool = False
) -> list[DatabaseInfo]:
    """
    `db_name` must be url encoded twice to get around a deficiency in ASGI.
    """

    db_name = process_db_name(db_name)

    dbs = [
        DatabaseInfo(path=db.path)
        for db in HIPPODB.databases[app_view.app_id].values()
        if db.path.startswith(db_name)
    ]

    if not recursive:
        query_length = len(db_name)
        dbs = [db for db in dbs if db.path[query_length:].count("/") == 0]

    return dbs


@application.get("/{db_name}", tags=["database"])
def list_documents(app_view: ApplicationDependency, db_name: str) -> list[str]:
    """
    `db_name` must be url encoded twice to get around a deficiency in ASGI.
    """

    db_name = process_db_name(db_name)

    return list(
        HIPPODB.documents[app_view.app_id][
            HIPPODB.databases[app_view.app_id][db_name].id
        ].keys()
    )


# @application.put("/{db_name}", tags=["database"])
# def update_database(db_name: str):
#     """
#     `db_name` must be url encoded twice to get around a deficiency in ASGI.
#     """

#     db_name = process_db_name(db_name)


@application.delete("/{db_name}", tags=["database"])
def delete_database(app_view: ApplicationDependency, db_name: str) -> None:
    """
    `db_name` must be url encoded twice to get around a deficiency in ASGI.
    """

    db_name = process_db_name(db_name)

    HIPPODB.delete_database(
        app_view.app_id, HIPPODB.databases[app_view.app_id][db_name].id
    )


### Documents ###


@application.post("/{db_name}", tags=["document"])
async def new_document(
    app_view: ApplicationDependency,
    body: dict[str, Any] | list[Any],
    db_name: str,
    document_name: str | None = None,
) -> Annotated[str, "Document name"]:
    """
    `db_name` must be url encoded twice to get around a deficiency in ASGI.
    """

    db_name = process_db_name(db_name)

    document_name = document_name or str(uuid4())

    HIPPODB.update_document(
        app_view.app_id,
        HIPPODB.databases[app_view.app_id][db_name].id,
        document_name,
        orjson.dumps(body).decode(),
    )

    return document_name


@application.get("/{db_name}/{document_name}", tags=["document"])
def read_document(
    app_view: ApplicationDependency, db_name: str, document_name: str
) -> list[Any] | dict[str, Any]:
    """
    `db_name` and `document_name` must be url encoded twice to get around a deficiency in ASGI.
    """

    db_name, document_name = process_db_name(db_name), unquote(document_name)

    return orjson.loads(
        HIPPODB.read_document(
            app_view.app_id,
            HIPPODB.databases[app_view.app_id][db_name].id,
            document_name,
        )
    )


@application.get("/{db_name}/{document_name}/exists", tags=["document"])
def document_exists(
    app_view: ApplicationDependency, db_name: str, document_name: str
) -> bool:
    """
    `db_name` and `document_name` must be url encoded twice to get around a deficiency in ASGI.
    """

    db_name, document_name = process_db_name(db_name), unquote(document_name)

    return (
        document_name
        in HIPPODB.documents[app_view.app_id][
            HIPPODB.databases[app_view.app_id][db_name].id
        ]
    )


@application.put("/{db_name}/{document_name}", tags=["document"])
def update_document(
    app_view: ApplicationDependency,
    db_name: str,
    document_name: str,
    body: dict[str, Any] | list[Any],
) -> None:
    """
    `db_name` and `document_name` must be url encoded twice to get around a deficiency in ASGI.
    """

    db_name, document_name = process_db_name(db_name), unquote(document_name)

    HIPPODB.update_document(
        app_view.app_id,
        HIPPODB.databases[app_view.app_id][db_name].id,
        document_name,
        orjson.dumps(body).decode(),
    )


@application.delete("/{db_name}/{document_name}", tags=["document"])
def delete_document(
    app_view: ApplicationDependency, db_name: str, document_name: str
) -> list[Any] | dict[str, Any]:
    """
    `db_name` and `document_name` must be url encoded twice to get around a deficiency in ASGI.
    """

    db_name, document_name = process_db_name(db_name), unquote(document_name)

    return orjson.loads(
        HIPPODB.delete_document(
            app_view.app_id,
            HIPPODB.databases[app_view.app_id][db_name].id,
            document_name,
        )
    )
