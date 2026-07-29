from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "import-colab-artifact.py"
SPEC = importlib.util.spec_from_file_location("xpde_import_colab_artifact", SCRIPT)
assert SPEC and SPEC.loader
importer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(importer)


def make_artifact(path: Path, *, eligible: bool = True) -> Path:
    path.mkdir()
    manifest = {
        "schema_version": 3,
        "model_id": "candidate-test-v3",
        "feature_version": "goldm-m5-v2",
        "eligible_for_shadow": eligible,
        "barrier_spec": {
            "id": "atr-1.25tp-1.00sl-h3-v1",
            "horizon_bars": 3,
        },
    }
    (path / "manifest.json").write_text(
        json.dumps(manifest) + "\n", encoding="utf-8"
    )
    (path / "payload.bin").write_bytes(b"verified")
    checksums = []
    for filename in ("manifest.json", "payload.bin"):
        digest = hashlib.sha256((path / filename).read_bytes()).hexdigest()
        checksums.append(f"{digest}  {filename}")
    (path / "checksums.sha256").write_text(
        "\n".join(checksums) + "\n", encoding="utf-8"
    )
    return path


def test_imports_immutable_verified_candidate(tmp_path) -> None:
    artifact = make_artifact(tmp_path / "source")
    target = importer.import_candidate(
        artifact,
        tmp_path / "artifacts",
        promote_latest=False,
    )
    assert target.name == "candidate-test-v3"
    assert (target / "payload.bin").read_bytes() == b"verified"
    with pytest.raises(FileExistsError):
        importer.import_candidate(
            artifact,
            tmp_path / "artifacts",
            promote_latest=False,
        )


def test_ineligible_candidate_is_not_copied_to_latest(tmp_path) -> None:
    artifact = make_artifact(tmp_path / "source", eligible=False)
    with pytest.raises(ValueError, match="eligibility"):
        importer.import_candidate(
            artifact,
            tmp_path / "artifacts",
            promote_latest=True,
        )
    assert not (tmp_path / "artifacts" / "runs").exists()
