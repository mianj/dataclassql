from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from typed_db.codegen import generate_client

__datasource__ = {"provider": "sqlite", "url": None}


@pytest.mark.skipif(os.environ.get("SKIP_PYRIGHT_TESTS") == "1", reason="pyright check skipped")
def test_pyright_reports_missing_required_field(tmp_path: Path) -> None:
    db_path = tmp_path / "pyright.db"

    global __datasource__
    __datasource__ = {"provider": "sqlite", "url": f"sqlite:///{db_path.as_posix()}"}

    from dataclasses import dataclass

    @dataclass
    class User:
        id: int | None
        name: str
        email: str

    module = generate_client([User])
    client_path = tmp_path / "client_module.py"
    client_path.write_text(module.code, encoding="utf-8")

    snippet = tmp_path / "snippet.py"
    snippet.write_text(
        """
from client_module import GeneratedClient

client = GeneratedClient()
client.user.insert({"name": "Alice", "email": "a@example.com"})
client.user.insert({"email": "missing"})
""",
        encoding="utf-8",
    )

    env = os.environ.copy()
    pythonpath_entries = [tmp_path.as_posix(), env.get("PYTHONPATH", "")]
    env["PYTHONPATH"] = ":".join([entry for entry in pythonpath_entries if entry])

    result = subprocess.run(
        ["uv", "run", "pyright", str(snippet)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode != 0

    assert 'Argument of type "dict[str, str]" cannot be assigned to parameter "data" of type "UserInsert | UserInsertDict" in function "insert"' in result.stdout, result.stdout
    assert 'reportArgumentType' in result.stdout, result.stdout
