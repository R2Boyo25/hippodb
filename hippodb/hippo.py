from dataclasses import dataclass
from pprint import pprint
import shutil
from uuid import uuid4
import os
import pathlib
from typing import Any, TypeVar, TypedDict, cast
from dataclasses_json import DataClassJsonMixin
import orjson


@dataclass
class Database(DataClassJsonMixin):
    id: str
    application: str
    path: str


@dataclass
class Application(DataClassJsonMixin):
    id: str
    name: str


@dataclass
class Token(DataClassJsonMixin):
    id: str
    application: str
    writeable: bool


Applications = TypedDict(
    "Applications",
    {"applications": list[dict[Any, Any]], "tokens": list[dict[Any, Any]]},
)
DBMap = dict[str, Database]
DocumentMap = dict[str, str]


class HippoDB:
    def __init__(self):
        self.hippo_dir = pathlib.Path(os.environ.get("HIPPODB_DIR", "hippo_data"))
        self.applications: dict[str, Application] = {}
        self.tokens: dict[str, Token] = {}
        self.databases: dict[str, dict[str, Database]] = {}
        self.documents: dict[str, dict[str, DocumentMap]] = {}

        self.load()

    def create_token(self, application: str, writeable: bool = False) -> Token:
        token_id = str(uuid4())

        self.tokens[token_id] = Token(token_id, application, writeable)
        self.save_applications_file()

        return self.tokens[token_id]

    def create_application(self, name: str) -> Application:
        app_id = str(uuid4())

        self.applications[app_id] = Application(app_id, name)
        self.databases[app_id] = {}
        self.documents[app_id] = {}
        self.save_applications_file()
        self.create_database(app_id, "/")

        return self.applications[app_id]

    def create_database(self, application: str, path: str) -> Database:
        db_id = str(uuid4())

        self.databases[application][path] = Database(db_id, application, path)
        self.documents[application][db_id] = {}
        self.save_db_map(application)
        self.save_document_map(application, db_id)

        return self.databases[application][path]

    def update_document(
        self, application: str, database: str, document_name: str, contents: str
    ) -> None:
        if document_name not in self.documents[application][database]:
            document_id = str(uuid4())
            self.documents[application][database][document_name] = document_id
            self.save_document_map(application, database)

        else:
            document_id = self.documents[application][database][document_name]

        (self.hippo_dir / "db" / application / database / document_id).write_text(
            contents, encoding="utf-8"
        )

    def read_document(self, application: str, database: str, document_name: str) -> str:
        document_id = self.documents[application][database][document_name]

        return (self.hippo_dir / "db" / application / database / document_id).read_text(
            encoding="utf-8"
        )

    def delete_document(
        self, application: str, database: str, document_name: str
    ) -> str:
        document_id = self.documents[application][database][document_name]
        document_file = self.hippo_dir / "db" / application / database / document_id

        contents = document_file.read_text(encoding="utf-8")

        document_file.unlink()

        self.save_document_map(application, database)

        return contents

    def delete_database(self, application: str, database: str) -> None:
        del self.databases[application][database]
        del self.documents[application][database]

        shutil.rmtree(self.hippo_dir / "db" / application / database)

        self.save_db_map(application)

    def delete_application(self, application: str) -> None:
        del self.databases[application]
        del self.documents[application]
        del self.applications[application]

        tokens = [
            token_id
            for token_id, token in self.tokens.items()
            if token.application == application
        ]

        for token_id in tokens:
            del self.tokens[token_id]

        shutil.rmtree(self.hippo_dir / "db" / application)
        self.save_applications_file()

    def delete_token(self, token: str) -> None:
        del self.tokens[token]
        self.save_applications_file()

    def load(self) -> None:
        self.load_applications_file()

        for application in self.applications:
            self.load_db_map(application)

            for database in self.databases[application].values():
                self.load_document_map(application, database.id)

        pprint(
            {
                "applications": self.applications,
                "tokens": self.tokens,
                "databases": self.databases,
                "documents": self.documents,
            },
            compact=True,
        )

    def load_applications_file(self) -> None:
        if not self.hippo_dir.exists():
            self.hippo_dir.mkdir(parents=True)

        if not (self.hippo_dir / "applications.json").exists():
            self.save_applications_file()
            return

        data = cast(
            Applications,
            orjson.loads((self.hippo_dir / "applications.json").read_text()),
        )

        T = TypeVar("T", Application, Token)

        def load_field(t: type[T], field: str) -> dict[str, T]:
            return dict(
                [
                    (value.id, value)
                    for value in t.schema().load(
                        data[field],
                        many=True,
                    )
                ]
            )

        self.applications = load_field(Application, "applications")
        self.tokens = load_field(Token, "tokens")

    def load_db_map(self, application: str) -> None:
        app_dir = self.hippo_dir / "db" / application

        if not app_dir.exists():
            app_dir.mkdir(parents=True)

        db_map = app_dir / "map.json"

        self.databases.setdefault(application, {})
        self.documents.setdefault(application, {})

        if not db_map.exists():
            self.save_db_map(application)

            return

        for db in (
            Database.schema().load(db)
            for db in orjson.loads(db_map.read_text(encoding="utf-8")).values()
        ):
            self.databases[application][db.path] = db

    def load_document_map(self, application: str, database: str) -> None:
        db_dir = self.hippo_dir / "db" / application / database

        if not db_dir.exists():
            db_dir.mkdir(parents=True)

        document_map = db_dir / "map.json"

        self.documents.setdefault(application, {})

        if not document_map.exists():
            self.documents[application][database] = {}
            self.save_document_map(application, database)
            return

        self.documents[application][database] = cast(
            dict[str, str], orjson.loads(document_map.read_text(encoding="utf-8"))
        )

    def save(self) -> None:
        if not self.hippo_dir.exists():
            self.hippo_dir.mkdir(parents=True)

        self.save_applications_file()

        for application in self.applications:
            self.save_db_map(application)

            for database in self.databases[application].values():
                self.save_document_map(application, database.id)

    def save_applications_file(self) -> None:
        (self.hippo_dir / "applications.json").write_bytes(
            orjson.dumps(
                Applications(
                    {
                        "applications": [
                            Application.schema().dump(app)
                            for app in self.applications.values()
                        ],
                        "tokens": [
                            Token.schema().dump(token) for token in self.tokens.values()
                        ],
                    }
                )
            ),
        )

    def save_db_map(self, application: str) -> None:
        app_dir = self.hippo_dir / "db" / application

        if not app_dir.exists():
            app_dir.mkdir(parents=True)

        (app_dir / "map.json").write_bytes(
            orjson.dumps(
                dict(
                    (db.id, Database.schema().dump(db))
                    for db in self.databases[application].values()
                )
            )
        )

    def save_document_map(self, application: str, database: str) -> None:
        db_dir = self.hippo_dir / "db" / application / database

        if not db_dir.exists():
            db_dir.mkdir(parents=True)

        (db_dir / "map.json").write_bytes(
            orjson.dumps(self.documents[application][database])
        )

    def cleanup(self): ...
