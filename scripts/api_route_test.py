#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import asyncio
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
        self.searches: list[tuple[str, int]] = []
        self.table_reads: list[tuple[str, str, int]] = []

    async def truncate_table(self, schema: str, table: str) -> None:
        self.truncated_tables.append((schema, table))

    async def truncate_schema(self, schema: str) -> dict[str, object]:
        self.truncated_schemas.append(schema)
        return {"truncated_tables": [f"{schema}.example"], "count": 1}

    async def search_account_profiles(self, query: str, limit: int) -> list[dict[str, object]]:
        self.searches.append((query, limit))
        return []

    async def get_table_data(self, schema: str, table: str, limit: int) -> dict[str, object]:
        self.table_reads.append((schema, table, limit))
        return {"rows": [], "count": 0}


def main_test() -> None:
    dummy_db = DummyDatabase()
    main.db = dummy_db
    route_auth = {"username": "route-test-admin", "site_owner": True, "admin_mode": True, "role": "admin"}
    main._require_api_auth = lambda request: route_auth
    main._require_admin_auth = lambda request: route_auth

    client = TestClient(main.app, base_url="http://127.0.0.1:8000")

    favicon = client.get("/favicon.ico")
    assert favicon.status_code == 200, favicon.text
    assert favicon.headers["content-type"].startswith("image/vnd.microsoft.icon")

    session = client.get("/api/session", headers={"X-Request-ID": "panel-route-test"})
    assert session.status_code == 200, session.text
    assert session.headers["x-request-id"] == "panel-route-test"
    assert "server-timing" in session.headers
    assert session.headers["cache-control"] == "no-store"
    assert session.headers["pragma"] == "no-cache"
    assert session.headers["x-permitted-cross-domain-policies"] == "none"
    assert session.json()["authenticated"] is False

    health = client.get("/api/health", headers={"X-Request-ID": "panel-health-test"})
    assert health.status_code == 200, health.text
    assert health.json()["request_id"] == "panel-health-test"
    assert health.json()["api_max_rows"] == main.settings.api_max_rows

    directory = client.get("/api/users/directory?q= route   tester &limit=999")
    assert directory.status_code == 200, directory.text
    assert directory.json()["limit"] == 50
    assert dummy_db.searches[-1] == ("route tester", 50)

    table_data = client.get("/api/database/data?schema_name=discord_music_gws&table_name=gws_queue&limit=9999")
    assert table_data.status_code == 200, table_data.text
    assert dummy_db.table_reads[-1] == ("discord_music_gws", "gws_queue", main.settings.api_max_rows)

    assert main._bounded_query_limit(9999, default=24, max_limit=50) == 50
    too_long_source = "x" * (main.settings.bot_control_source_max_chars + 1)
    try:
        main._normalize_control_source(too_long_source)
    except ValueError as exc:
        assert "PLAY source" in str(exc)
    else:
        raise AssertionError("oversized PLAY source was accepted")
    try:
        asyncio.run(main._normalize_bot_control_request(main.BotControlRequest(bot_key="LOCKHART", guild_id="1", action="PAUSE", payload={"surprise": True})))
    except ValueError as exc:
        assert "Unsupported control payload" in str(exc)
    else:
        raise AssertionError("unexpected control payload key was accepted")

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
