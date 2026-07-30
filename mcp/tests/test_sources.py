from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from conftest import MODEL_ID, PREDICTION_ID
from xpde_mcp.config import Settings
from xpde_mcp.database import ReadOnlyDatabase
from xpde_mcp.errors import (
    AccessDeniedError,
    ConfigurationError,
    ContractError,
)
from xpde_mcp.manifest import ManifestReader


@pytest.mark.parametrize(
    "value",
    [
        "http://example.com:8787",
        "http://user:secret@127.0.0.1:8787",
        "http://127.0.0.1:8787/api",
        "http://127.0.0.1:8787?route=x",
        "file:///tmp/xpde",
    ],
)
def test_api_base_is_loopback_origin_only(tmp_path: Path, value: str) -> None:
    with pytest.raises(ConfigurationError):
        Settings.create(
            api_base=value,
            database_path=tmp_path / "db",
            artifact_root=tmp_path,
        )


def test_sqlite_connection_is_read_only_and_query_only(service_fixture) -> None:
    database = ReadOnlyDatabase(service_fixture["database_path"])
    before = service_fixture["database_path"].stat().st_mtime_ns
    with database.connect() as connection:
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute(
                "INSERT INTO audit_events(event_type,payload_json,created_at) "
                "VALUES ('MCP_WRITE','{}','now')"
            )
    assert service_fixture["database_path"].stat().st_mtime_ns == before


def test_recent_prediction_filters_are_bounded(service_fixture) -> None:
    service = service_fixture["service"]
    predictions = service.get_recent_predictions(
        model_id=MODEL_ID,
        settlement_status="settled",
        limit=5,
    )
    assert [item["prediction_id"] for item in predictions] == [PREDICTION_ID]
    assert predictions[0]["forecast"]["model_id"] == MODEL_ID
    with pytest.raises(ContractError):
        service.get_recent_predictions(limit=201)
    with pytest.raises(ContractError):
        service.get_recent_predictions(settlement_status="DROP TABLE predictions")


def test_prediction_evidence_is_joined_without_writes(service_fixture) -> None:
    evidence = service_fixture["service"].get_prediction_evidence(PREDICTION_ID)
    assert evidence["prediction"]["prediction_id"] == PREDICTION_ID
    assert [row["horizon_bars"] for row in evidence["horizon_outcomes"]] == [
        1,
        3,
        6,
        12,
    ]
    assert evidence["proposal_outcomes"][0]["barrier_outcome"] == "TP_FIRST"
    assert evidence["evidence_memberships"][0]["evidence_source"] == "FIRST_ACTIONABLE"
    assert evidence["human_feedback"][0]["verdict"] == "ACCEPTED"
    with pytest.raises(ContractError):
        service_fixture["service"].get_prediction_evidence("not-a-uuid")


def test_read_adapter_tolerates_pre_contract_optional_columns(service_fixture) -> None:
    database_path = service_fixture["database_path"]
    connection = sqlite3.connect(database_path)
    connection.execute("ALTER TABLE predictions DROP COLUMN label_contract_id")
    connection.execute("ALTER TABLE predictions DROP COLUMN settlement_reason")
    connection.execute(
        "ALTER TABLE decision_proposal_instances DROP COLUMN settlement_status"
    )
    connection.execute(
        "ALTER TABLE decision_proposal_instances DROP COLUMN settlement_reason"
    )
    connection.commit()
    connection.close()

    database = ReadOnlyDatabase(database_path)
    recent = database.recent_predictions(limit=1)
    assert recent[0]["label_contract_id"] is None
    assert recent[0]["settlement_reason"] is None
    evidence = database.prediction_evidence(PREDICTION_ID)
    assert evidence["proposal_instances"][0]["settlement_status"] == "UNKNOWN"
    assert evidence["proposal_instances"][0]["settlement_reason"] is None


def test_manifest_reader_blocks_path_escape(tmp_path: Path) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "manifest.json").write_text("{}", encoding="utf-8")
    reader = ManifestReader(root)
    with pytest.raises(AccessDeniedError):
        reader.read_registered(
            model_id=MODEL_ID,
            models=[{"model_id": MODEL_ID, "artifact_path": str(outside)}],
        )


def test_registered_manifest_is_the_only_artifact_file_read(service_fixture) -> None:
    result = service_fixture["service"].get_model_manifest(MODEL_ID)
    assert result["manifest"]["model_id"] == MODEL_ID
    assert result["source"] == {
        "kind": "MODEL_MANIFEST",
        "artifact_root_enforced": True,
        "manifest_file": "manifest.json",
    }


def test_relative_registered_manifest_is_resolved_inside_artifact_root(
    service_fixture,
) -> None:
    reader = ManifestReader(service_fixture["artifact_root"])
    result = reader.read_registered(
        model_id=MODEL_ID,
        models=[{"model_id": MODEL_ID, "artifact_path": "run"}],
    )
    assert result["manifest"]["model_id"] == MODEL_ID


def test_tool_contract_declares_no_write_or_listener() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "xpde_mcp"
        / "schemas"
        / "tool-contracts.json"
    )
    contract = json.loads(path.read_text(encoding="utf-8"))
    assert contract["transport"] == "STDIO"
    assert contract["network_listener_created"] is False
    assert contract["source_boundaries"]["rest"]["methods"] == ["GET"]
    assert contract["source_boundaries"]["sqlite"] == {
        "mode": "ro",
        "query_only": True,
    }
    assert "ORDER_EXECUTION" in contract["forbidden_capabilities"]
    assert all(
        token not in name
        for name in contract["tools"]
        for token in ("execute", "feedback", "promote", "train", "register")
    )


def test_implementation_has_no_rest_write_or_network_server_entrypoint() -> None:
    source_root = Path(__file__).resolve().parents[1] / "src" / "xpde_mcp"
    api_source = (source_root / "api.py").read_text(encoding="utf-8")
    server_source = (source_root / "server.py").read_text(encoding="utf-8")
    assert "._client.get(" in api_source
    assert all(
        token not in api_source
        for token in (
            "._client.post(",
            "._client.put(",
            "._client.patch(",
            "._client.delete(",
        )
    )
    assert 'mcp.run(transport="stdio")' in server_source
    assert all(
        token not in server_source
        for token in (
            "streamable_http",
            "sse_app",
            "uvicorn.run",
        )
    )
