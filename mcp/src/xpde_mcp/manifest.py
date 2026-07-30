"""Artifact manifest reader restricted to the configured artifact root."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .contracts import MAXIMUM_MANIFEST_BYTES
from .errors import AccessDeniedError, ContractError, NotFoundError


class ManifestReader:
    def __init__(self, artifact_root: Path) -> None:
        self.artifact_root = artifact_root.resolve()

    def _manifest_path(self, artifact_path: str) -> Path:
        raw = Path(artifact_path).expanduser()
        if raw.is_absolute():
            candidate = raw.resolve()
        else:
            root_relative = (self.artifact_root / raw).resolve()
            repository_relative = (self.artifact_root.parents[1] / raw).resolve()
            candidate = next(
                (
                    path
                    for path in (root_relative, repository_relative)
                    if path.exists()
                ),
                root_relative,
            )
        try:
            candidate.relative_to(self.artifact_root)
        except ValueError as error:
            raise AccessDeniedError(
                "registered artifact is outside XPDE_ARTIFACT_ROOT"
            ) from error
        manifest = (candidate / "manifest.json").resolve()
        try:
            manifest.relative_to(self.artifact_root)
        except ValueError as error:
            raise AccessDeniedError(
                "manifest resolves outside artifact root"
            ) from error
        if not manifest.is_file():
            raise NotFoundError(f"manifest not found for artifact: {candidate.name}")
        if manifest.stat().st_size > MAXIMUM_MANIFEST_BYTES:
            raise ContractError("manifest exceeds the read-only size limit")
        return manifest

    def read_registered(
        self,
        *,
        model_id: str,
        models: list[dict[str, Any]],
    ) -> dict[str, Any]:
        record = next(
            (item for item in models if item.get("model_id") == model_id), None
        )
        if record is None:
            raise NotFoundError(f"model not registered: {model_id}")
        artifact_path = record.get("artifact_path")
        if not isinstance(artifact_path, str) or not artifact_path:
            raise ContractError("registered model has no artifact_path")
        manifest_path = self._manifest_path(artifact_path)
        try:
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ContractError(f"cannot read model manifest: {error}") from error
        if not isinstance(value, dict):
            raise ContractError("model manifest must be a JSON object")
        manifest_model_id = value.get("model_id")
        if manifest_model_id is not None and manifest_model_id != model_id:
            raise ContractError("manifest model_id does not match registry")
        return {
            "registry": record,
            "manifest": value,
            "source": {
                "kind": "MODEL_MANIFEST",
                "artifact_root_enforced": True,
                "manifest_file": "manifest.json",
            },
        }
