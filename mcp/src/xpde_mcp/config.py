"""Environment-only configuration with local and path boundaries."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .errors import ConfigurationError

_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _validated_api_base(value: str) -> str:
    candidate = value.strip().rstrip("/")
    parsed = urlparse(candidate)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname not in _LOOPBACK_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ConfigurationError(
            "XPDE_API_BASE must be a loopback HTTP(S) origin without credentials, "
            "query, fragment, or path"
        )
    return candidate


@dataclass(frozen=True)
class Settings:
    """Read-only source locations for the STDIO server."""

    api_base: str
    database_path: Path
    artifact_root: Path
    request_timeout_seconds: float = 5.0

    @classmethod
    def from_env(cls) -> Settings:
        return cls.create(
            api_base=os.environ.get("XPDE_API_BASE", "http://127.0.0.1:8787"),
            database_path=os.environ.get("XPDE_DB_PATH", "data/xpde.sqlite"),
            artifact_root=os.environ.get("XPDE_ARTIFACT_ROOT", "artifacts/catboost"),
            request_timeout_seconds=float(
                os.environ.get("XPDE_MCP_TIMEOUT_SECONDS", "5")
            ),
        )

    @classmethod
    def create(
        cls,
        *,
        api_base: str,
        database_path: str | Path,
        artifact_root: str | Path,
        request_timeout_seconds: float = 5.0,
    ) -> Settings:
        if not (0.1 <= request_timeout_seconds <= 30.0):
            raise ConfigurationError(
                "XPDE_MCP_TIMEOUT_SECONDS must be between 0.1 and 30 seconds"
            )
        return cls(
            api_base=_validated_api_base(api_base),
            database_path=Path(database_path).expanduser().resolve(),
            artifact_root=Path(artifact_root).expanduser().resolve(),
            request_timeout_seconds=request_timeout_seconds,
        )
