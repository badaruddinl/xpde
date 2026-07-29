from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
import uuid
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "ml"))

from xpde_ml.dataset import (
    BARRIER_HORIZON,
    BARRIER_SPEC_ID,
    EXECUTABLE_SIDE_CONTRACT_ID,
    FEATURE_VERSION,
    LABEL_CONTRACT_ID,
)
from xpde_ml.contracts import HORIZONS
from xpde_ml.model_inference import (
    REQUIRED_ARTIFACT_FILES,
    CandidateModel,
    candidate_registration_payload,
    verify_artifact_checksums,
)


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


def _golden_snapshot() -> dict:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    bars = []
    close = 3300.0
    for index in range(500):
        opening = close
        close = opening + 0.12 * math.sin(index / 7.0) + 0.03
        bars.append(
            {
                "timestamp": (start + timedelta(minutes=5 * index))
                .isoformat()
                .replace("+00:00", "Z"),
                "open": opening,
                "high": max(opening, close) + 0.35,
                "low": min(opening, close) - 0.35,
                "close": close,
                "tick_volume": 1000.0 + index,
            }
        )
    return {
        "symbol": "GOLDm#",
        "timeframe": "M5",
        "bid": close - 0.12,
        "ask": close + 0.12,
        "symbol_spec": {"tick_size": 0.01, "digits": 2},
        "bars": bars,
    }


def _verify_golden_forecast(path: Path) -> None:
    snapshot = _golden_snapshot()
    forecast = CandidateModel(path).forecast(snapshot)
    numeric_fields = (
        "origin_close",
        "direction_probability_up",
        "barrier_probability_long",
        "barrier_probability_short",
        "target_price_long",
        "stop_price_long",
        "target_price_short",
        "stop_price_short",
    )
    if any(not math.isfinite(float(forecast[field])) for field in numeric_fields):
        raise ValueError("golden forecast contains a non-finite value")
    if (
        forecast["barrier_spec_id"] != BARRIER_SPEC_ID
        or int(forecast["barrier_horizon_bars"]) != BARRIER_HORIZON
        or forecast.get("label_contract_id") != LABEL_CONTRACT_ID
        or forecast.get("probability_reference") != "FORECAST_ORIGIN"
        or forecast.get("entry_conditioned_probability") is not False
        or forecast["stop_price_long"] >= forecast["origin_close"]
        or forecast["target_price_long"] <= forecast["origin_close"]
        or forecast["target_price_short"] >= forecast["origin_close"]
        or forecast["stop_price_short"] <= forecast["origin_close"]
    ):
        raise ValueError("golden forecast violates the barrier contract")
    tick_size = float(snapshot["symbol_spec"]["tick_size"])
    if any(
        not math.isclose(
            float(forecast[field]) / tick_size,
            round(float(forecast[field]) / tick_size),
            rel_tol=0.0,
            abs_tol=1e-7,
        )
        for field in (
            "target_price_long",
            "stop_price_long",
            "target_price_short",
            "stop_price_short",
        )
    ):
        raise ValueError("golden forecast barriers are not broker-tick aligned")
    if [int(point.get("horizon_bars", 0)) for point in forecast["points"]] != list(
        HORIZONS
    ):
        raise ValueError("golden forecast must contain exactly H1, H3, H6 and H12")
    for point in forecast["points"]:
        quantiles = [point[f"q{quantile}"] for quantile in (10, 25, 50, 75, 90)]
        if any(not math.isfinite(float(value)) for value in quantiles):
            raise ValueError("golden forecast contains a non-finite quantile")
        if quantiles != sorted(quantiles):
            raise ValueError("golden forecast contains crossing quantiles")


def verify_candidate(path: Path) -> dict:
    verify_artifact_checksums(path)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if int(manifest.get("schema_version", 0)) != 3:
        raise ValueError("only schema v3 artifacts can be imported")
    if int(manifest.get("eligibility_gate_version", 0)) < 3:
        raise ValueError("artifact eligibility gate version is incompatible")
    if manifest.get("training_mode") != "candidate":
        raise ValueError("only candidate-mode artifacts can be imported")
    if manifest.get("eligible_for_shadow") is not True:
        raise ValueError("candidate did not pass the shadow eligibility gates")
    gates = manifest.get("eligibility_gates")
    if not isinstance(gates, dict) or not gates or not all(
        value is True for value in gates.values()
    ):
        raise ValueError("candidate contains missing or failed eligibility gates")
    declared_files = set(manifest.get("artifact_files") or ())
    actual_files = {entry.name for entry in path.iterdir() if entry.is_file()}
    if (
        declared_files != REQUIRED_ARTIFACT_FILES
        or actual_files != REQUIRED_ARTIFACT_FILES
    ):
        raise ValueError("artifact required-file set is incomplete or unexpected")
    checksum_files = {
        line.partition("  ")[2]
        for line in (path / "checksums.sha256")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    }
    if checksum_files != REQUIRED_ARTIFACT_FILES - {"checksums.sha256"}:
        raise ValueError("artifact checksums do not cover every required file")
    if manifest.get("feature_version") != FEATURE_VERSION:
        raise ValueError("artifact feature version is incompatible")
    if manifest.get("label_contract_id") != LABEL_CONTRACT_ID:
        raise ValueError("artifact label contract is incompatible")
    barrier = manifest.get("barrier_spec", {})
    if (
        barrier.get("id") != BARRIER_SPEC_ID
        or int(barrier.get("horizon_bars", 0)) != BARRIER_HORIZON
        or barrier.get("price_alignment") != "BROKER_TICK_SIZE_OUTWARD"
        or float(barrier.get("tick_size", 0.0)) <= 0.0
    ):
        raise ValueError("artifact barrier contract is incompatible")
    executable_side = manifest.get("executable_side_contract", {})
    if (
        executable_side.get("id") != EXECUTABLE_SIDE_CONTRACT_ID
        or executable_side.get("chart_mode") != "BID"
        or executable_side.get("long_exit_ohlc") != "BID"
        or executable_side.get("short_exit_ohlc") != "ASK"
        or executable_side.get("source") != "HISTORICAL_BID_ASK_TICKS"
        or float(executable_side.get("minimum_tick_coverage", 0.0)) < 0.95
    ):
        raise ValueError("artifact executable-side contract is incompatible")
    model_id = str(manifest.get("model_id", "")).strip()
    if not model_id or Path(model_id).name != model_id:
        raise ValueError("artifact model_id is invalid")
    _verify_golden_forecast(path)
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
        target = artifacts_root / "runs" / manifest["model_id"]
        if target.exists():
            raise FileExistsError(f"immutable candidate already exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(candidate_root, target)
        verify_candidate(target)

        if promote_latest:
            staging = artifacts_root / f".latest-{uuid.uuid4().hex}"
            shutil.copytree(target, staging)
            verify_candidate(staging)
            latest = artifacts_root / "latest"
            backup = None
            if latest.exists():
                timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
                backup = artifacts_root / f"previous-{timestamp}-{uuid.uuid4().hex[:6]}"
                latest.rename(backup)
            try:
                staging.rename(latest)
                verify_candidate(latest)
            except Exception:
                if latest.exists():
                    shutil.rmtree(latest)
                if backup is not None and backup.exists():
                    backup.rename(latest)
                raise
    return target


def register_candidate(target: Path, register_url: str) -> str:
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    payload = candidate_registration_payload(manifest, target)
    request = urllib.request.Request(
        register_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            if response.status not in (200, 201, 202):
                raise RuntimeError(
                    f"model registry rejected candidate with HTTP {response.status}"
                )
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"model registry rejected candidate with HTTP {error.code}: {body}"
        ) from error
    return "registered"


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
    parser.add_argument(
        "--register-url",
        default="http://127.0.0.1:8787/api/v1/models/register",
    )
    args = parser.parse_args()
    target = import_candidate(
        args.source.resolve(),
        args.artifacts_root.resolve(),
        promote_latest=not args.no_promote_latest,
    )
    registration = "disabled"
    if args.register_url:
        try:
            registration = register_candidate(target, args.register_url)
        except urllib.error.URLError:
            registration = "pending_core_unavailable"
            print(
                "Candidate imported safely; model registry is offline. "
                "XPDE-Start.cmd will register it when the candidate is loaded.",
                file=sys.stderr,
            )
    print(
        json.dumps(
            {
                "imported": str(target),
                "promoted_latest": not args.no_promote_latest,
                "registration": registration,
            }
        )
    )


if __name__ == "__main__":
    main()
