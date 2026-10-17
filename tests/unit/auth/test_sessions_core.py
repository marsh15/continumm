"""Pure boundaries for OAuth state/session inputs; DB proof is in the PG suite."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
from pydantic import SecretStr

from continuum_api.auth.sessions import (
    AuthStorageError,
    OAuthRateLimiter,
    OAuthStateStore,
    SessionService,
    _valid_opaque,
)
from continuum_api.credentials import (
    CredentialCipher,
    CredentialError,
    CredentialProvider,
    CredentialScope,
)
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.provider_credentials import CredentialService


def test_opaque_tokens_reject_unbounded_or_non_url_safe_values() -> None:
    assert _valid_opaque("a" * 43)
    assert not _valid_opaque("short")
    assert not _valid_opaque("a" * 129)
    assert not _valid_opaque("a" * 42 + "/")


def test_state_consumption_and_rate_limit_fail_closed_for_invalid_inputs() -> None:
    # Invalid inputs are rejected before either service touches its DB handle.
    state_store = OAuthStateStore(object(), SecretStr("s" * 32))  # type: ignore[arg-type]
    limiter = OAuthRateLimiter(object(), SecretStr("s" * 32))  # type: ignore[arg-type]
    assert state_store.consume("malformed", "malformed") is None
    assert not limiter.allow("not-an-ip-address")


def test_session_login_rejects_invalid_identity_before_database_access() -> None:
    service = SessionService(object(), CredentialCipher("v1", {"v1": os.urandom(32)}))  # type: ignore[arg-type]
    with pytest.raises(AuthStorageError, match="github_oauth_identity_invalid"):
        service.login(0, "", SecretStr("synthetic-token-value"), 28_800)


def test_github_cipher_domain_is_separate_from_model_providers() -> None:
    assert CredentialProvider.GITHUB.value == "github"
    cipher = CredentialCipher("v1", {"v1": os.urandom(32)})
    service = CredentialService(object(), Principal(uuid4()), cipher)  # type: ignore[arg-type]
    with pytest.raises(CredentialError, match="credential_identity_invalid"):
        service.save(
            CredentialProvider.GITHUB,
            SecretStr("synthetic-github-token-value"),
            scope=CredentialScope.SAVED,
            saved_key_opt_in=True,
        )
