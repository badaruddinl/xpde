from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import uuid
import zipfile
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "ml"))

from xpde_ml.dataset import BARRIER_HORIZON, BARRIER_SPEC_ID, FEATURE_VERSION
from xpde_ml.model_inference import verify_artifact_checksums


def _safe_extract(archive: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive) as bundle:
        destination_resolved = destination.resolve()
        for member in bundle.infolist():
            target = (destination / member.filename).resolve()
            if destination_resolved not in target.parents and target != destination_resolved:
                raise ValueError(f"archive contains an unsafe path: {member.filename}")
        bundle.extractall(destination)


def _artifact_root(path: Path) -> Path:
    if (path / "manifest.json").is_file():
        return path
    matches = list(path.glob("*/manifest.json"))
    if len(matches) != 1:
        raise ValueError("archive must contain exactly one artifact manifest")
    return matches[0].parent


def verify_candidate(path: Path) -> dict:
    verify_artifact_checksums(path)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if int(manifest.get("schema_version", 0)) < 3:
        raise ValueError("only schema v3 artifacts can be imported")
    if manifest.get("feature_version") != FEATURE_VERSION:
        raise ValueError("artifact feature version is incompatible")
    barrier = manifest.get("barrier_spec", {})
    if (
        barrier.get("id") != BARRIER_SPEC_ID
        or int(barrier.get("horizon_bars", 0)) != BARRIER_HORIZON
    ):
        raise ValueError("artifact barrier contract is incompatible")
    model_id = str(manifest.get("model_id", "")).strip()
    if not model_id or Path(model_id).name != model_id:
        raise ValueError("artifact model_id is invalid")
    return manifest


def import_candidate(
    source: Path,
    artifacts_root: Path,
    *,
    promote_latest: bool = True,
) -> Path:
    artifacts_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="xpde-colab-import-") as temporary:
        temporary_path = Path(temporary)
        if source.is_dir():
            candidate_root = source
        elif source.suffix.lower() == ".zip":
            _safe_extract(source, temporary_path)
            candidate_root = _artifact_root(temporary_path)
        else:
            raise ValueError("source must be an artifact directory or ZIP archive")

        manifest = verify_candidate(candidate_root)
        if promote_latest and manifest.get("eligible_for_shadow") is not True:
            raise ValueError("candidate did not pass the shadow eligibility gates")
        target = artifacts_root / "runs" / manifest["model_id"]
        if target.exists():
            raise FileExistsError(f"immutable candidate already exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(candidate_root, target)
        verify_candidate(target)

        if promote_latest:
            staging = artifacts_root / f".latest-{uuid.uuid4().hex}"
            shutil.copytree(target, staging)
            latest = artifacts_root / "latest"
            if latest.exists():
                timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
                backup = artifacts_root / f"previous-{timestamp}-{uuid.uuid4().hex[:6]}"
                latest.rename(backup)
            staging.rename(latest)
            verify_candidate(latest)
    return target


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify and import an immutable XPDE Colab artifact"
    )
    parser.add_argument("source", type=Path)
    parser.add_argument(
        "--artifacts-root",
        type=Path,
        default=REPO_ROOT / "artifacts" / "catboost",
    )
    parser.add_argument("--no-promote-latest", action="store_true")
    args = parser.parse_args()
    target = import_candidate(
        args.source.resolve(),
        args.artifacts_root.resolve(),
        promote_latest=not args.no_promote_latest,
    )
    print(json.dumps({"imported": str(target), "promoted_latest": not args.no_promote_latest}))


if __name__ == "__main__":
    main()
