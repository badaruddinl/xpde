"""Explicit errors returned by read-only adapters."""


class XpdeMcpError(RuntimeError):
    """Base error for a safe, user-readable MCP failure."""


class ConfigurationError(XpdeMcpError):
    """The local read-only source configuration is invalid."""


class SourceUnavailableError(XpdeMcpError):
    """An XPDE source cannot be read."""


class ContractError(XpdeMcpError):
    """Source data violates a contract required for deterministic analysis."""


class NotFoundError(XpdeMcpError):
    """A requested XPDE entity does not exist."""


class AccessDeniedError(XpdeMcpError):
    """A path or operation falls outside the read-only boundary."""
