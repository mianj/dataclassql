from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dclassql.cli import DEFAULT_MODEL_FILE, main


MODEL_TEMPLATE = """
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

__datasource__ = {{
    "provider": "sqlite",
    "url": "sqlite:///{db_path}",
    "name": {datasource_name!r},
}}

@dataclass
class User:
    id: int | None
    name: str
    email: str | None
    created_at: datetime

    def index(self):
        yield self.name
        yield self.created_at
"""


def write_model(tmp_path: Path, db_path: Path, name: str | None = None) -> Path:
    module_path = tmp_path / DEFAULT_MODEL_FILE
    module_path.write_text(
        MODEL_TEMPLATE.format(
            db_path=db_path.as_posix(),
            datasource_name=name if name is not None else "None",
        ),
        encoding="utf-8",
    )
    return module_path


def test_generate_command_outputs_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "example.db"
    module_path = write_model(tmp_path, db_path)
    exit_code = main(["-m", str(module_path), "generate"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "class GeneratedClient" in captured.out
    assert "UserTable" in captured.out


def test_push_db_command_creates_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "push.sqlite"
    module_path = write_model(tmp_path, db_path, name="main")
    exit_code = main(["-m", str(module_path), "push-db"])
    assert exit_code == 0

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='User'"
        ).fetchone()[0]
        assert count == 1
    finally:
        conn.close()
