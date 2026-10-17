"""Hashed browser sessions, one-use OAuth state, and owner-bound GitHub tokens."""

from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import secrets
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.credentials import (
    CredentialCipher,
    CredentialIdentity,
    CredentialProvider,
    CredentialScope,
)
from continuum_api.runtime.persistence import Principal

SESSION_SECONDS = 7 * 24 * 60 * 60
OAUTH_STATE_SECONDS = 10 * 60
AUTH_ATTEMPT_LIMIT = 10
AUTH_ATTEMPT_WINDOW_SECONDS = 15 * 60


class AuthStorageError(RuntimeError):
    """Safe database/authentication boundary failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class AuthPrincipal:
    principal: Principal
    github_user_id: int
    login: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class OAuthState:
    state: str = field(repr=False)
    browser_binding: str = field(repr=False)
    code_verifier: SecretStr = field(repr=False)


@dataclass(frozen=True, slots=True)
class SessionCredentials:
    session_token: str = field(repr=False)
    csrf_token: str = field(repr=False)
    principal: AuthPrincipal


class OAuthStateStore:
    """Persist only state/browser hashes and derive the PKCE verifier server-side."""

    def __init__(self, engine: Engine, pkce_key: SecretStr) -> None:
        if (
            not isinstance(pkce_key, SecretStr)
            or len(pkce_key.get_secret_value().encode("utf-8")) < 32
        ):
            raise ValueError("github_oauth_configuration_invalid")
        self._engine = engine
        self._key = pkce_key.get_secret_value().encode("utf-8")

    def issue(self) -> OAuthState:
        state = secrets.token_urlsafe(32)
        browser_binding = secrets.token_urlsafe(32)
        verifier = _b64url(hmac.new(self._key, b"pkce\0" + state.encode(), hashlib.sha256).digest())
        try:
            with self._engine.begin() as connection:
                connection.execute(
                    text(
                        "SELECT runtime_private.auth_issue_oauth_state(:state_hash, :browser_hash)"
                    ),
                    {
                        "state_hash": _sha256(state),
                        "browser_hash": _sha256(browser_binding),
                    },
                )
        except SQLAlchemyError:
            raise AuthStorageError("github_oauth_state_unavailable") from None
        return OAuthState(state, browser_binding, SecretStr(verifier))

    def consume(self, state: str, browser_binding: str) -> SecretStr | None:
        if not _valid_opaque(state) or not _valid_opaque(browser_binding):
            return None
        try:
            with self._engine.begin() as connection:
                accepted: bool = connection.execute(
                    text(
                        "SELECT runtime_private.auth_consume_oauth_state(:state_hash, "
                        ":browser_hash)"
                    ),
                    {
                        "state_hash": _sha256(state),
                        "browser_hash": _sha256(browser_binding),
                    },
                ).scalar_one()
        except SQLAlchemyError:
            raise AuthStorageError("github_oauth_state_unavailable") from None
        if not accepted:
            return None
        verifier = _b64url(hmac.new(self._key, b"pkce\0" + state.encode(), hashlib.sha256).digest())
        return SecretStr(verifier)


class OAuthRateLimiter:
    """Shared database-backed callback/start limiter keyed by hashed client IP."""

    def __init__(self, engine: Engine, pkce_key: SecretStr) -> None:
        if (
            not isinstance(pkce_key, SecretStr)
            or len(pkce_key.get_secret_value().encode("utf-8")) < 32
        ):
            raise ValueError("github_oauth_configuration_invalid")
        self._engine = engine
        self._key = pkce_key.get_secret_value().encode("utf-8")

    def allow(self, address: str) -> bool:
        try:
            normalized = str(ipaddress.ip_address(address))
        except (TypeError, ValueError):
            return False
        digest = hmac.new(
            self._key, b"rate\0" + normalized.encode("ascii"), hashlib.sha256
        ).digest()
        try:
            with self._engine.begin() as connection:
                return bool(
                    connection.execute(
                        text(
                            "SELECT runtime_private.auth_allow_oauth_attempt(:address_hash, "
                            ":attempt_limit, :window_seconds)"
                        ),
                        {
                            "address_hash": digest,
                            "attempt_limit": AUTH_ATTEMPT_LIMIT,
                            "window_seconds": AUTH_ATTEMPT_WINDOW_SECONDS,
                        },
                    ).scalar_one()
                )
        except SQLAlchemyError:
            # If the shared limiter is unavailable, auth starts fail closed.
            raise AuthStorageError("github_oauth_rate_limit_unavailable") from None


class SessionService:
    """Create/resolve/revoke opaque browser sessions and account GitHub tokens."""

    def __init__(self, engine: Engine, cipher: CredentialCipher) -> None:
        self._engine = engine
        self._cipher = cipher

    def login(
        self,
        github_user_id: int,
        login: str,
        access_token: SecretStr,
        token_expires_in: int,
        prior_session_token: str | None = None,
    ) -> SessionCredentials | None:
        if (
            not isinstance(github_user_id, int)
            or isinstance(github_user_id, bool)
            or github_user_id <= 0
            or not isinstance(login, str)
            or len(login) > 39
            or not login.isascii()
            or not login.replace("-", "").isalnum()
            or not 1 <= len(login) <= 39
            or not isinstance(token_expires_in, int)
            or isinstance(token_expires_in, bool)
            or not 1 <= token_expires_in <= 28_800
            or not isinstance(access_token, SecretStr)
        ):
            raise AuthStorageError("github_oauth_identity_invalid")
        proposed_owner_id = uuid4()
        try:
            with self._engine.begin() as connection:
                owner_id = connection.execute(
                    text(
                        "SELECT runtime_private.auth_upsert_github_user("
                        ":new_owner, :github_user_id, :login)"
                    ),
                    {
                        "new_owner": proposed_owner_id,
                        "github_user_id": github_user_id,
                        "login": login,
                    },
                ).scalar_one_or_none()
        except SQLAlchemyError:
            raise AuthStorageError("github_oauth_login_unavailable") from None
        if owner_id is None:
            return None
        token_id = uuid4()
        identity = CredentialIdentity(
            token_id, owner_id, CredentialProvider.GITHUB, CredentialScope.SAVED
        )
        envelope = self._cipher.encrypt(identity, access_token)
        session_token = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(32)
        prior_hash = None
        if prior_session_token is not None:
            if not _valid_opaque(prior_session_token):
                raise AuthStorageError("github_oauth_session_invalid")
            prior_hash = _sha256(prior_session_token)
        try:
            with self._engine.begin() as connection:
                row = (
                    connection.execute(
                        text(
                            "SELECT * FROM runtime_private.auth_complete_github_login("
                            ":owner_id, :github_user_id, :login, :token_id, :key_version, "
                            ":nonce, :ciphertext, :token_expires_in, :session_hash, "
                            ":csrf_hash, :prior_session_hash)"
                        ),
                        {
                            "owner_id": owner_id,
                            "github_user_id": github_user_id,
                            "login": login,
                            "token_id": token_id,
                            "key_version": envelope.key_version,
                            "nonce": envelope.nonce,
                            "ciphertext": envelope.ciphertext,
                            "token_expires_in": token_expires_in,
                            "session_hash": _sha256(session_token),
                            "csrf_hash": _sha256(csrf_token),
                            "prior_session_hash": prior_hash,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
        except SQLAlchemyError:
            raise AuthStorageError("github_oauth_login_unavailable") from None
        if row is None:
            return None
        principal = AuthPrincipal(
            Principal(row["user_id"]),
            int(row["github_user_id"]),
            str(row["login"]),
            row["expires_at"],
        )
        return SessionCredentials(session_token, csrf_token, principal)

    def resolve(self, session_token: str) -> AuthPrincipal | None:
        if not _valid_opaque(session_token):
            return None
        try:
            with self._engine.begin() as connection:
                row = (
                    connection.execute(
                        text(
                            "SELECT user_id, github_user_id, login, expires_at, csrf_valid "
                            "FROM runtime_private.auth_resolve_session(:session_hash, NULL)"
                        ),
                        {"session_hash": _sha256(session_token)},
                    )
                    .mappings()
                    .one_or_none()
                )
        except SQLAlchemyError:
            raise AuthStorageError("github_oauth_session_unavailable") from None
        if row is None:
            return None
        return _auth_principal(row)

    def validate_csrf(self, session_token: str, csrf_token: str) -> bool:
        if not _valid_opaque(session_token) or not _valid_opaque(csrf_token):
            return False
        try:
            with self._engine.begin() as connection:
                return bool(
                    connection.execute(
                        text(
                            "SELECT csrf_valid FROM runtime_private.auth_resolve_session("
                            ":session_hash, :csrf_hash)"
                        ),
                        {
                            "session_hash": _sha256(session_token),
                            "csrf_hash": _sha256(csrf_token),
                        },
                    ).scalar_one_or_none()
                )
        except SQLAlchemyError:
            raise AuthStorageError("github_oauth_session_unavailable") from None

    def revoke(self, session_token: str) -> None:
        if not _valid_opaque(session_token):
            return
        try:
            with self._engine.begin() as connection:
                connection.execute(
                    text("SELECT runtime_private.auth_revoke_session(:session_hash)"),
                    {"session_hash": _sha256(session_token)},
                )
        except SQLAlchemyError:
            raise AuthStorageError("github_oauth_session_unavailable") from None

    def revoke_account(self, owner_id: UUID) -> None:
        if not isinstance(owner_id, UUID):
            raise AuthStorageError("github_oauth_session_invalid")
        try:
            with self._engine.begin() as connection:
                connection.execute(
                    text("SELECT runtime_private.auth_revoke_account(:owner_id)"),
                    {"owner_id": owner_id},
                )
        except SQLAlchemyError:
            raise AuthStorageError("github_oauth_session_unavailable") from None


class GitHubUserTokenStore:
    """Read an active, encrypted GitHub token for one bound internal account."""

    def __init__(self, engine: Engine, cipher: CredentialCipher, owner_id: UUID) -> None:
        if not isinstance(owner_id, UUID):
            raise ValueError("github_token_owner_invalid")
        self._engine = engine
        self._cipher = cipher
        self._owner_id = owner_id

    def get_for_user(self, github_user_id: int) -> SecretStr | None:
        if (
            not isinstance(github_user_id, int)
            or isinstance(github_user_id, bool)
            or github_user_id <= 0
        ):
            return None
        try:
            with self._engine.begin() as connection:
                row = (
                    connection.execute(
                        text(
                            "SELECT * FROM runtime_private.auth_get_github_token("
                            ":owner_id, :github_user_id)"
                        ),
                        {"owner_id": self._owner_id, "github_user_id": github_user_id},
                    )
                    .mappings()
                    .one_or_none()
                )
        except SQLAlchemyError:
            raise AuthStorageError("github_token_unavailable") from None
        if row is None:
            return None
        identity = CredentialIdentity(
            row["token_id"], self._owner_id, CredentialProvider.GITHUB, CredentialScope.SAVED
        )
        from continuum_api.credentials import CredentialEnvelope

        return self._cipher.decrypt(
            identity,
            CredentialEnvelope(
                str(row["key_version"]), bytes(row["nonce"]), bytes(row["ciphertext"])
            ),
        )


def _auth_principal(row: Any) -> AuthPrincipal:
    return AuthPrincipal(
        Principal(row["user_id"]),
        int(row["github_user_id"]),
        str(row["login"]),
        row["expires_at"],
    )


def _valid_opaque(value: str) -> bool:
    return (
        isinstance(value, str)
        and 32 <= len(value) <= 128
        and all(char.isascii() and (char.isalnum() or char in "-_") for char in value)
    )


def _sha256(value: str) -> bytes:
    return hashlib.sha256(value.encode("ascii")).digest()


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")
