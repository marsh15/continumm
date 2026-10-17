"""GitHub's maintained OAuth client integration with strict response bounds."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field

import httpx2
from authlib.integrations.httpx_client import AsyncOAuth2Client  # type: ignore[import-untyped]
from authlib.oauth2.client import OAuth2Client  # type: ignore[import-untyped]
from pydantic import SecretStr

from continuum_api.auth.config import AuthConfig

_AUTHORIZATION_ENDPOINT = "https://github.com/login/oauth/authorize"
_TOKEN_ENDPOINT = "https://github.com/login/oauth/access_token"
_USER_ENDPOINT = "https://api.github.com/user"
_MAX_RESPONSE_BYTES = 64 * 1024
_HANDLE = re.compile(r"^[A-Za-z0-9-]{1,39}$")
_VERIFIER = re.compile(r"^[A-Za-z0-9._~-]{43,128}$")


class GitHubOAuthError(RuntimeError):
    """Safe OAuth failures that never include provider response data."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class GitHubOAuthResult:
    github_user_id: int
    login: str
    access_token: SecretStr = field(repr=False)
    expires_in: int


class GitHubOAuthClient:
    """Exchange PKCE authorization codes and fetch the immutable GitHub ID.

    The callback URL and provider endpoints are fixed by configuration/code. The
    client follows no redirects and uses no retrying transport for token exchange.
    """

    def __init__(
        self,
        config: AuthConfig,
        transport: httpx2.AsyncBaseTransport | None = None,
    ) -> None:
        self._config = config
        self._transport = transport

    def authorization_url(self, state: str, code_verifier: str) -> str:
        if not _valid_token(state, 32, 128) or not _VERIFIER.fullmatch(code_verifier):
            raise GitHubOAuthError("github_oauth_request_invalid")
        client = OAuth2Client(
            None,
            client_id=self._config.client_id,
            code_challenge_method="S256",
            redirect_uri=self._config.callback_uri,
            scope="offline_access",
        )
        url, _ = client.create_authorization_url(
            _AUTHORIZATION_ENDPOINT,
            state=state,
            code_verifier=code_verifier,
            redirect_uri=self._config.callback_uri,
            scope="offline_access",
            allow_signup="false",
        )
        return str(url)

    async def exchange_code(self, code: str, code_verifier: str) -> GitHubOAuthResult:
        if not _valid_token(code, 1, 1024) or not _VERIFIER.fullmatch(code_verifier):
            raise GitHubOAuthError("github_oauth_request_invalid")

        async with self._client() as client:
            try:
                async with asyncio.timeout(20):
                    token = await client.fetch_token(
                        _TOKEN_ENDPOINT,
                        grant_type="authorization_code",
                        code=code,
                        redirect_uri=self._config.callback_uri,
                        code_verifier=code_verifier,
                    )
                    access_token = token.get("access_token")
                    expires_in = token.get("expires_in")
                    token_type = token.get("token_type", "bearer")
                    if (
                        not isinstance(access_token, str)
                        or not _valid_token(access_token, 20, 4096)
                        or not isinstance(expires_in, int)
                        or isinstance(expires_in, bool)
                        or not 1 <= expires_in <= 28_800
                        or not isinstance(token_type, str)
                        or token_type.lower() != "bearer"
                    ):
                        raise GitHubOAuthError("github_oauth_token_invalid")

                    response = await client.get(
                        _USER_ENDPOINT,
                        headers={
                            "Accept": "application/vnd.github+json",
                            "X-GitHub-Api-Version": "2026-03-10",
                        },
                    )
                    if response.status_code != 200:
                        raise GitHubOAuthError("github_oauth_identity_unavailable")
                    try:
                        payload = response.json()
                    except ValueError:
                        raise GitHubOAuthError("github_oauth_identity_invalid") from None
                    if not isinstance(payload, dict):
                        raise GitHubOAuthError("github_oauth_identity_invalid")
                    github_user_id = payload.get("id")
                    login = payload.get("login")
                    if (
                        not isinstance(github_user_id, int)
                        or isinstance(github_user_id, bool)
                        or github_user_id <= 0
                        or not isinstance(login, str)
                        or _HANDLE.fullmatch(login) is None
                    ):
                        raise GitHubOAuthError("github_oauth_identity_invalid")
                    return GitHubOAuthResult(
                        github_user_id,
                        login,
                        SecretStr(access_token),
                        expires_in,
                    )
            except GitHubOAuthError:
                raise
            except Exception:
                # Suppress Authlib/httpx exceptions, which can carry request data.
                raise GitHubOAuthError("github_oauth_unavailable") from None

    def _client(self) -> AsyncOAuth2Client:
        return AsyncOAuth2Client(
            client_id=self._config.client_id,
            client_secret=self._config.client_secret.get_secret_value(),
            redirect_uri=self._config.callback_uri,
            scope="offline_access",
            token_endpoint_auth_method="client_secret_post",
            code_challenge_method="S256",
            transport=_BoundedTransport(
                self._transport or httpx2.AsyncHTTPTransport(trust_env=False, retries=0)
            ),
            timeout=httpx2.Timeout(8.0, connect=5.0, write=5.0, pool=3.0),
            trust_env=False,
            follow_redirects=False,
        )


def _valid_token(value: str, minimum: int, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and minimum <= len(value) <= maximum
        and all(0x21 <= ord(char) <= 0x7E for char in value)
    )


class _BoundedTransport(httpx2.AsyncBaseTransport):
    """Buffer only small OAuth responses, including chunked responses."""

    def __init__(self, transport: httpx2.AsyncBaseTransport) -> None:
        self._transport = transport

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        response = await self._transport.handle_async_request(request)
        declared = response.headers.get("content-length")
        if declared is not None:
            try:
                if int(declared) > _MAX_RESPONSE_BYTES:
                    await response.aclose()
                    raise GitHubOAuthError("github_oauth_response_too_large")
            except ValueError:
                await response.aclose()
                raise GitHubOAuthError("github_oauth_response_invalid") from None
        body = bytearray()
        try:
            async for chunk in response.aiter_bytes():
                if len(body) + len(chunk) > _MAX_RESPONSE_BYTES:
                    raise GitHubOAuthError("github_oauth_response_too_large")
                body.extend(chunk)
            headers = {
                key: value
                for key, value in response.headers.items()
                if key.lower() not in {"content-length", "transfer-encoding", "content-encoding"}
            }
            bounded = httpx2.Response(
                response.status_code,
                headers=headers,
                content=bytes(body),
                request=request,
                extensions=response.extensions,
            )
            await response.aclose()
            return bounded
        except BaseException:
            await response.aclose()
            raise

    async def aclose(self) -> None:
        await self._transport.aclose()
