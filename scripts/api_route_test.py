#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("PANEL_DB_PASSWORD", "route-test-db-password")
os.environ.setdefault("PANEL_ADMIN_PASSWORD", "route-test-admin-password")
os.environ.setdefault("PANEL_SESSION_SECRET", "route-test-session-secret-with-enough-entropy")
os.environ.setdefault("PANEL_DESTRUCTIVE_CONFIRMATION_PHRASE", "route-test-owner-confirm")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

import app.main as main


class DummyDatabase:
    def __init__(self) -> None:
        self.truncated_tables: list[tuple[str, str]] = []
        self.truncated_schemas: list[str] = []

    async def truncate_table(self, schema: str, table: str) -> None:
        self.truncated_tables.append((schema, table))

    async def truncate_schema(self, schema: str) -> dict[str, object]:
        self.truncated_schemas.append(schema)
        return {"truncated_tables": [f"{schema}.example"], "count": 1}


def main_test() -> None:
    dummy_db = DummyDatabase()
    main.db = dummy_db
    main._require_admin_auth = lambda request: {"username": "route-test-admin", "site_owner": True, "admin_mode": True}

    client = TestClient(main.app, base_url="http://127.0.0.1:8000")

    favicon = client.get("/favicon.ico")
    assert favicon.status_code == 200, favicon.text
    assert favicon.headers["content-type"].startswith("image/vnd.microsoft.icon")

    session = client.get("/api/session")
    assert session.status_code == 200, session.text
    assert session.json()["authenticated"] is False

    missing_owner = client.post(
        "/api/database/truncate-table",
        json={
            "schema_name": "discord_music_gws",
            "table_name": "gws_queue",
            "confirm_text": "TRUNCATE discord_music_gws.gws_queue",
        },
    )
    assert missing_owner.status_code == 400, missing_owner.text
    assert dummy_db.truncated_tables == []

    table = client.post(
        "/api/database/truncate-table",
        json={
            "schema_name": "discord_music_gws",
            "table_name": "gws_queue",
            "confirm_text": "TRUNCATE discord_music_gws.gws_queue",
            "owner_confirm_text": main.settings.destructive_confirmation_phrase,
        },
    )
    assert table.status_code == 200, table.text
    assert dummy_db.truncated_tables == [("discord_music_gws", "gws_queue")]

    schema = client.post(
        "/api/database/truncate-schema",
        json={
            "schema_name": "discord_music_gws",
            "confirm_text": "TRUNCATE ALL discord_music_gws",
            "owner_confirm_text": main.settings.destructive_confirmation_phrase,
        },
    )
    assert schema.status_code == 200, schema.text
    assert dummy_db.truncated_schemas == ["discord_music_gws"]

    print("swarmpanel_api_routes=passed")


if __name__ == "__main__":
    main_test()
