from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

import pytest

from typed_db.codegen import generate_client
from typed_db.push import db_push


__datasource__ = {"provider": "sqlite", "url": "sqlite:///runtime.db"}


@dataclass
class RuntimeUser:
    id: int | None
    name: str
    email: str | None


def _build_client(connection: Any):
    module = generate_client([RuntimeUser])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    generated_client = namespace["GeneratedClient"]
    client = generated_client({"sqlite": connection})
    return namespace, client


def test_insert_and_find_roundtrip():
    conn = sqlite3.connect(":memory:")
    db_push([RuntimeUser], {"sqlite": conn})
    namespace, client = _build_client(conn)
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    stored = user_table.insert(InsertModel(id=None, name="Alice", email="alice@example.com"))
    assert stored.id is not None
    assert stored.name == "Alice"
    assert stored.email == "alice@example.com"

    stored_dict = user_table.insert({"id": None, "name": "Bob", "email": None})
    assert stored_dict.name == "Bob"
    assert stored_dict.email is None

    fetched = user_table.find_many(where={"name": "Alice"})
    assert [user.name for user in fetched] == ["Alice"]

    ordered = user_table.find_many(order_by=[("name", "desc")])
    assert [user.name for user in ordered] == ["Bob", "Alice"]

    first = user_table.find_first(order_by=[("name", "asc")])
    assert first.name == "Alice"


def test_insert_many_utilises_backend():
    conn = sqlite3.connect(":memory:")
    db_push([RuntimeUser], {"sqlite": conn})
    namespace, client = _build_client(conn)
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    rows = [
        InsertModel(id=None, name="Carol", email=None),
        {"id": None, "name": "Dave", "email": "dave@example.com"},
    ]
    inserted = user_table.insert_many(rows, batch_size=1)
    assert [user.name for user in inserted] == ["Carol", "Dave"]

    all_rows = user_table.find_many(order_by=[("name", "asc")])
    assert [user.name for user in all_rows] == ["Carol", "Dave"]


def test_insert_many_generates_sequential_ids():
    conn = sqlite3.connect(":memory:")
    db_push([RuntimeUser], {"sqlite": conn})
    namespace, client = _build_client(conn)
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    inserted = user_table.insert_many(
        [
            InsertModel(id=None, name="Foo", email=None),
            InsertModel(id=None, name="Bar", email=None),
            InsertModel(id=None, name="Baz", email=None),
        ],
        batch_size=2,
    )

    ids = [user.id for user in inserted]
    assert ids == [1, 2, 3]


def test_backend_accepts_factory_and_caches_connection():
    conn = sqlite3.connect(":memory:")
    db_push([RuntimeUser], {"sqlite": conn})
    calls: list[int] = []

    def factory() -> sqlite3.Connection:
        calls.append(1)
        return conn

    namespace, client = _build_client(factory)
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table.insert(InsertModel(id=None, name="Eve", email=None))
    user_table.find_first()

    assert len(calls) == 1


def test_find_many_rejects_unknown_columns():
    conn = sqlite3.connect(":memory:")
    db_push([RuntimeUser], {"sqlite": conn})
    _, client = _build_client(conn)
    user_table = client.runtime_user

    with pytest.raises(KeyError):
        user_table.find_many(where={"unknown": "value"})

    with pytest.raises(ValueError):
        user_table.find_many(order_by=[("name", "sideways")])


def test_include_not_supported():
    conn = sqlite3.connect(":memory:")
    db_push([RuntimeUser], {"sqlite": conn})
    _, client = _build_client(conn)
    user_table = client.runtime_user

    with pytest.raises(NotImplementedError):
        user_table.find_many(include={"anything": True})
