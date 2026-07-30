"""Fixed-route, GET-only client for the local XPDE Rust API."""

from __future__ import annotations

from typing import Any

import httpx

from .errors import ContractError, SourceUnavailableError

_READ_ONLY_ROUTES = {
    "state": "/api/v1/state",
    "evaluation": "/api/v1/evaluation/summary",
    "models": "/api/v1/models",
}


class XpdeApiClient:
    """A deliberately narrow API adapter with no arbitrary URL or write method."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout_seconds,
            follow_redirects=False,
            trust_env=False,
            transport=transport,
            headers={"Accept": "application/json"},
        )

    def close(self) -> None:
        self._client.close()

    def _get(self, route_name: str) -> Any:
        path = _READ_ONLY_ROUTES[route_name]
        try:
            response = self._client.get(path)
            response.raise_for_status()
            value = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise SourceUnavailableError(
                f"XPDE API GET {path} failed: {error}"
            ) from error
        if not isinstance(value, (dict, list)):
            raise ContractError(f"XPDE API GET {path} did not return JSON object/list")
        return value

    def get_state(self) -> dict[str, Any]:
        value = self._get("state")
        if not isinstance(value, dict):
            raise ContractError("XPDE state must be a JSON object")
        return value

    def get_evaluation_summary(self) -> dict[str, Any]:
        value = self._get("evaluation")
        if not isinstance(value, dict):
            raise ContractError("XPDE evaluation summary must be a JSON object")
        return value

    def get_models(self) -> list[dict[str, Any]]:
        value = self._get("models")
        if not isinstance(value, list) or not all(
            isinstance(item, dict) for item in value
        ):
            raise ContractError("XPDE model registry must be a JSON array of objects")
        return value
