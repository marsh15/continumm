"""Validated configuration for the fixed GitHub OAuth callback."""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import SplitResult, urlsplit

from pydantic import SecretStr


@dataclass(frozen=True, slots=True)
class AuthConfig:
    client_id: str
    client_secret: SecretStr = field(repr=False)
    redirect_uri: str
    public_origin: str
    pkce_key: SecretStr = field(repr=False)
    environment: str = "production"

    def __post_init__(self) -> None:
        if not isinstance(self.client_id, str) or not 1 <= len(self.client_id) <= 128:
            raise ValueError("github_oauth_configuration_invalid")
        if (
            not isinstance(self.client_secret, SecretStr)
            or not self.client_secret.get_secret_value()
        ):
            raise ValueError("github_oauth_configuration_invalid")
        if (
            not isinstance(self.pkce_key, SecretStr)
            or len(self.pkce_key.get_secret_value().encode("utf-8")) < 32
        ):
            raise ValueError("github_oauth_configuration_invalid")
        if self.environment not in {"development", "test", "production"}:
            raise ValueError("github_oauth_configuration_invalid")

        origin = _url(self.public_origin)
        callback = _url(self.redirect_uri)
        dev_http = self.environment in {"development", "test"} and _loopback(origin.hostname)
        allowed_schemes = {"https"} | ({"http"} if dev_http else set())
        if (
            origin.scheme not in allowed_schemes
            or origin.username is not None
            or origin.password is not None
            or origin.path not in {"", "/"}
            or origin.query
            or origin.fragment
            or callback.scheme != origin.scheme
            or callback.netloc.lower() != origin.netloc.lower()
            or callback.path != "/api/v1/auth/github/callback"
            or callback.username is not None
            or callback.password is not None
            or callback.query
            or callback.fragment
        ):
            raise ValueError("github_oauth_configuration_invalid")
        object.__setattr__(self, "public_origin", self.public_origin.rstrip("/"))

    @property
    def callback_uri(self) -> str:
        """Return the exact callback registered with GitHub."""
        return self.redirect_uri


def _url(value: str) -> SplitResult:
    try:
        parsed = urlsplit(value)
        # Accessing .port validates malformed ports and IPv6 brackets.
        _ = parsed.port
        if not parsed.scheme or not parsed.netloc or not parsed.hostname:
            raise ValueError
        return parsed
    except (AttributeError, TypeError, ValueError):
        raise ValueError("github_oauth_configuration_invalid") from None


def _loopback(host: str | None) -> bool:
    return host in {"localhost", "127.0.0.1", "::1"}
