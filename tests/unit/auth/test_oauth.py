"""Synthetic GitHub OAuth protocol boundary tests."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from urllib.parse import parse_qs, urlsplit

import httpx2
import pytest
from pydantic import SecretStr

from continuum_api.auth.config import AuthConfig
from continuum_api.auth.oauth import GitHubOAuthClient, GitHubOAuthError


def _config(**overrides: object) -> AuthConfig:
    fields: dict[str, object] = {
        "client_id": "synthetic-client-id",
        "client_secret": SecretStr("synthetic-client-secret"),
        "redirect_uri": "https://console.example/api/v1/auth/github/callback",
        "public_origin": "https://console.example",
        "pkce_key": SecretStr("synthetic-pkce-hmac-key-with-more-than-32-bytes"),
        "environment": "production",
    }
    fields.update(overrides)
    return AuthConfig(**fields)  # type: ignore[arg-type]


def test_auth_config_requires_fixed_same_origin_https_callback() -> None:
    with pytest.raises(ValueError, match="github_oauth_configuration_invalid"):
        _config(redirect_uri="https://attacker.example/api/v1/auth/github/callback")
    with pytest.raises(ValueError, match="github_oauth_configuration_invalid"):
        _config(redirect_uri="https://console.example/other?next=https://attacker.example")
    with pytest.raises(ValueError, match="github_oauth_configuration_invalid"):
        _config(
            public_origin="http://console.example",
            redirect_uri="http://console.example/api/v1/auth/github/callback",
        )


def test_authorization_url_uses_pkce_s256_and_fixed_redirect() -> None:
    client = GitHubOAuthClient(_config())
    verifier = "v" * 43
    url = client.authorization_url("s" * 43, verifier)
    query = parse_qs(urlsplit(url).query)
    expected_challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
    assert urlsplit(url).scheme == "https"
    assert urlsplit(url).netloc == "github.com"
    assert query["redirect_uri"] == ["https://console.example/api/v1/auth/github/callback"]
    assert query["state"] == ["s" * 43]
    assert query["code_challenge"] == [expected_challenge.rstrip(b"=").decode()]
    assert query["code_challenge_method"] == ["S256"]
    assert query["allow_signup"] == ["false"]
    assert query["scope"] == ["offline_access"]


def test_exchange_validates_provider_identity_and_does_not_return_refresh_token() -> None:
    class Transport(httpx2.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/login/oauth/access_token":
                form = parse_qs(request.content.decode())
                assert form["code_verifier"] == ["v" * 43]
                assert form["redirect_uri"] == [
                    "https://console.example/api/v1/auth/github/callback"
                ]
                return httpx2.Response(
                    200,
                    json={
                        "access_token": "synthetic-github-user-access-token",
                        "token_type": "bearer",
                        "expires_in": 28800,
                        "refresh_token": "synthetic-refresh-token-is-ignored",
                    },
                )
            assert request.url == "https://api.github.com/user"
            assert request.headers["authorization"] == "Bearer synthetic-github-user-access-token"
            return httpx2.Response(200, json={"id": 713, "login": "synthetic-user"})

    result = asyncio.run(
        GitHubOAuthClient(_config(), Transport()).exchange_code("synthetic-code", "v" * 43)
    )
    assert result.github_user_id == 713
    assert result.login == "synthetic-user"
    assert result.expires_in == 28800
    assert result.access_token.get_secret_value() == "synthetic-github-user-access-token"
    assert "synthetic-refresh-token-is-ignored" not in repr(result)


def test_exchange_rejects_malformed_identity_without_echoing_provider_payload() -> None:
    class Transport(httpx2.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/login/oauth/access_token":
                return httpx2.Response(
                    200,
                    json={
                        "access_token": "synthetic-github-user-access-token",
                        "expires_in": 28800,
                    },
                )
            return httpx2.Response(200, json={"id": 0, "login": "private-sentinel"})

    with pytest.raises(GitHubOAuthError, match="github_oauth_identity_invalid") as failure:
        asyncio.run(
            GitHubOAuthClient(_config(), Transport()).exchange_code("synthetic-code", "v" * 43)
        )
    assert "private-sentinel" not in str(failure.value)


def test_exchange_caps_chunked_response_bodies() -> None:
    class OversizedStream(httpx2.AsyncByteStream):
        async def __aiter__(self):
            yield b"x" * (32 * 1024)
            yield b"y" * (33 * 1024)

    class Transport(httpx2.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(200, stream=OversizedStream())

    with pytest.raises(GitHubOAuthError, match="github_oauth_response_too_large"):
        asyncio.run(
            GitHubOAuthClient(_config(), Transport()).exchange_code("synthetic-code", "v" * 43)
        )


def test_public_origin_trailing_slash_is_normalized() -> None:
    from dataclasses import replace

    config = replace(_config(), public_origin=_config().public_origin + "/")
    assert config.public_origin == _config().public_origin
