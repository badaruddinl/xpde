from __future__ import annotations

import hashlib

import pytest

from xpde_ml.model_inference import verify_artifact_checksums


def test_artifact_checksum_verification_detects_tampering(tmp_path) -> None:
    artifact = tmp_path / "manifest.json"
    artifact.write_text('{"schema_version": 2}\n', encoding="utf-8")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    (tmp_path / "checksums.sha256").write_text(
        f"{digest}  manifest.json\n",
        encoding="utf-8",
    )

    verify_artifact_checksums(tmp_path)
    artifact.write_text('{"schema_version": 3}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="checksum mismatch"):
        verify_artifact_checksums(tmp_path)


def test_artifact_checksum_rejects_path_traversal(tmp_path) -> None:
    (tmp_path / "checksums.sha256").write_text(
        f"{'0' * 64}  ../secret\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="invalid entry"):
        verify_artifact_checksums(tmp_path)
