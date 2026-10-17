"""Synthetic GitHub sessions and owner authorization; no live credentials used."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

# Reuse the audited, destructive loopback-only PostgreSQL 17 fixture.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "integration"))
from test_agent_loop import T22Database
from test_agent_loop import t22_database as t22_database

from continuum_api.app import create_app
from continuum_api.auth.boundary import (
    CSRF_COOKIE,
    SESSION_COOKIE,
    STATE_COOKIE,
    GitHubAuthBoundary,
)
from continuum_api.auth.config import AuthConfig
from continuum_api.auth.oauth import GitHubOAuthClient, GitHubOAuthResult
from continuum_api.auth.sessions import (
    GitHubUserTokenStore,
    OAuthRateLimiter,
    OAuthStateStore,
    SessionService,
)
from continuum_api.credentials import CredentialCipher

_ORIGIN = "https://console.fixture.invalid"
_STATE_KEY = SecretStr("t29-synthetic-state-key-never-a-deployment-secret")
_TOKEN = SecretStr("ghu_synthetic_GitHub_user_access_token_only")


def _config() -> AuthConfig:
    return AuthConfig(
        client_id="Iv1.synthetic-client",
        client_secret=SecretStr("synthetic-secret"),
        public_origin=_ORIGIN,
        redirect_uri=_ORIGIN + "/api/v1/auth/github/callback",
        pkce_key=_STATE_KEY,
    )


def _services(store: T22Database):
    cipher = CredentialCipher("v1", {"v1": os.urandom(32)})
    return SessionService(store.runtime_engine, cipher), cipher


def test_default_auth_does_not_trust_owner_headers(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path), base_url=_ORIGIN) as client:
        response = client.get(f"/api/v1/runs/{uuid4()}", headers={"X-User-ID": str(uuid4())})
    assert response.status_code == 401
    assert response.headers["x-content-type-options"] == "nosniff"
    assert "script-src 'self'" in response.headers["content-security-policy"]


def test_state_is_browser_bound_single_use_and_expiring(t22_database: T22Database) -> None:
    states = OAuthStateStore(t22_database.runtime_engine, _STATE_KEY)
    state = states.issue()
    assert states.consume(state.state, "x" * 43) is None
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(states.consume, state.state, state.browser_binding) for _ in range(2)
        ]
        results = [future.result(timeout=5) for future in futures]
    assert sum(result is not None for result in results) == 1
    assert states.consume(state.state, state.browser_binding) is None
    expired = states.issue()
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.github_oauth_states "
                "SET created_at=clock_timestamp()-interval '12 minutes', "
                "expires_at=clock_timestamp()-interval '1 minute' WHERE state_hash=:hash"
            ),
            {"hash": hashlib.sha256(expired.state.encode()).digest()},
        )
    assert states.consume(expired.state, expired.browser_binding) is None


def test_sessions_rotate_bind_numeric_identity_and_hide_token_material(
    t22_database: T22Database,
) -> None:
    sessions, cipher = _services(t22_database)
    github_id = int.from_bytes(os.urandom(6)) + 100
    first = sessions.login(github_id, "fixture-user", _TOKEN, 3600)
    assert first is not None
    assert sessions.resolve(first.session_token) == first.principal
    assert sessions.validate_csrf(first.session_token, first.csrf_token)
    assert not sessions.validate_csrf(first.session_token, "x" * 43)
    second_token = SecretStr("ghu_second_synthetic_user_access_token_only")
    second = sessions.login(github_id, "renamed-user", second_token, 3600, first.session_token)
    assert second is not None
    assert second.principal.principal == first.principal.principal
    assert second.session_token != first.session_token
    assert sessions.resolve(first.session_token) is None
    owner = second.principal.principal.account_id
    token_store = GitHubUserTokenStore(t22_database.runtime_engine, cipher, owner)
    assert token_store.get_for_user(github_id) == second_token
    assert (
        GitHubUserTokenStore(t22_database.runtime_engine, cipher, uuid4()).get_for_user(github_id)
        is None
    )
    with t22_database.admin_engine.connect() as connection:
        token = connection.execute(
            text("SELECT ciphertext FROM runtime_private.github_user_tokens WHERE owner_id=:owner"),
            {"owner": owner},
        ).scalar_one()
        stored = connection.execute(
            text(
                "SELECT session_hash,csrf_hash,expires_at-created_at "
                "FROM runtime_private.browser_sessions "
                "WHERE session_hash=:hash"
            ),
            {"hash": hashlib.sha256(second.session_token.encode()).digest()},
        ).one()
    assert second_token.get_secret_value().encode() not in bytes(token)
    assert stored[0] != second.session_token.encode() and stored[1] != second.csrf_token.encode()
    assert stored[2].total_seconds() == 7 * 86400
    with pytest.raises(DBAPIError):
        with t22_database.runtime_engine.begin() as connection:
            connection.execute(text("SELECT * FROM runtime_private.browser_sessions"))
    sessions.revoke(second.session_token)
    assert sessions.resolve(second.session_token) is None


def test_suspended_deleted_and_expired_accounts_cannot_authorize(t22_database: T22Database) -> None:
    sessions, cipher = _services(t22_database)
    for status in ("suspended", "deleting", "deleted"):
        github_id = int.from_bytes(os.urandom(6)) + 100
        session = sessions.login(github_id, "fixture-user", _TOKEN, 3600)
        assert session is not None
        owner = session.principal.principal.account_id
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text("UPDATE runtime_private.users SET status=:status WHERE user_id=:owner"),
                {"status": status, "owner": owner},
            )
        assert sessions.resolve(session.session_token) is None
        assert not sessions.validate_csrf(session.session_token, session.csrf_token)
        assert (
            GitHubUserTokenStore(t22_database.runtime_engine, cipher, owner).get_for_user(github_id)
            is None
        )
        assert sessions.login(github_id, "fixture-user", _TOKEN, 3600) is None


def test_shared_login_rate_limit_survives_new_service_instances(t22_database: T22Database) -> None:
    address = "192.0.2.49"
    for _ in range(10):
        assert OAuthRateLimiter(t22_database.runtime_engine, _STATE_KEY).allow(address)
    assert not OAuthRateLimiter(t22_database.runtime_engine, _STATE_KEY).allow(address)
    with t22_database.admin_engine.connect() as connection:
        hashes = (
            connection.execute(
                text("SELECT address_hash FROM runtime_private.github_oauth_rate_limits")
            )
            .scalars()
            .all()
        )
    assert all(len(value) == 32 and address.encode() not in bytes(value) for value in hashes)


def test_callback_rotation_and_csrf_logout_use_real_session_storage(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    from urllib.parse import parse_qs, urlsplit

    sessions, _cipher = _services(t22_database)
    github_id = int.from_bytes(os.urandom(6)) + 100

    class FakeOAuth(GitHubOAuthClient):
        calls = 0

        async def exchange_code(self, code: str, code_verifier: str) -> GitHubOAuthResult:
            self.calls += 1
            assert code == "synthetic-code" and len(code_verifier) >= 43
            return GitHubOAuthResult(github_id, "fixture-user", _TOKEN, 3600)

    oauth = FakeOAuth(_config())
    boundary = GitHubAuthBoundary(
        engine=t22_database.runtime_engine,
        config=_config(),
        sessions=sessions,
        states=OAuthStateStore(t22_database.runtime_engine, _STATE_KEY),
        oauth=oauth,
        limiter=OAuthRateLimiter(t22_database.runtime_engine, _STATE_KEY),
        authorization_factory=lambda principal: None,
    )
    app = create_app(
        tmp_path, runtime_engine=t22_database.runtime_engine, github_auth_boundary=boundary
    )
    with TestClient(
        app, base_url=_ORIGIN, client=("127.0.0.29", 12345), follow_redirects=False
    ) as client:
        start = client.get("/api/v1/auth/github/start")
        assert start.status_code == 303
        query = parse_qs(urlsplit(start.headers["location"]).query)
        assert query["code_challenge_method"] == ["S256"]
        assert "HttpOnly" in start.headers["set-cookie"] and "Secure" in start.headers["set-cookie"]
        callback = "/api/v1/auth/github/callback?code=synthetic-code&state=" + query["state"][0]
        accepted = client.get(callback)
        assert accepted.status_code == 303 and accepted.headers["location"] == "/"
        assert oauth.calls == 1
        session_token = client.cookies[SESSION_COOKIE]
        assert client.get("/api/v1/auth/session").json()["github_user_id"] == github_id
        client.cookies.set(
            STATE_COOKIE, start.cookies[STATE_COOKIE], domain="console.fixture.invalid", path="/"
        )
        assert client.get(callback).status_code == 400
        assert oauth.calls == 1
        assert client.post("/api/v1/auth/logout", headers={"Origin": _ORIGIN}).status_code == 403
        assert sessions.resolve(session_token) is not None
        assert (
            client.post(
                "/api/v1/auth/logout",
                headers={
                    "Origin": "https://foreign.invalid",
                    "X-CSRF-Token": client.cookies[CSRF_COOKIE],
                },
            ).status_code
            == 403
        )
        logged_out = client.post(
            "/api/v1/auth/logout",
            headers={
                "Origin": _ORIGIN,
                "X-CSRF-Token": client.cookies[CSRF_COOKIE],
            },
        )
        assert logged_out.status_code == 204
        assert sessions.resolve(session_token) is None
        assert client.get("/api/v1/auth/session").status_code == 401


def test_token_and_session_expiry_and_account_revocation(t22_database: T22Database) -> None:
    sessions, cipher = _services(t22_database)
    github_id = int.from_bytes(os.urandom(6)) + 100
    first = sessions.login(github_id, "fixture-user", _TOKEN, 3600)
    assert first is not None
    owner = first.principal.principal.account_id
    token_store = GitHubUserTokenStore(t22_database.runtime_engine, cipher, owner)
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.github_user_tokens "
                "SET expires_at=clock_timestamp()-interval '1 second' "
                "WHERE owner_id=:owner"
            ),
            {"owner": owner},
        )
    assert token_store.get_for_user(github_id) is None
    OAuthStateStore(t22_database.runtime_engine, _STATE_KEY).issue()
    with t22_database.admin_engine.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT ciphertext FROM runtime_private.github_user_tokens "
                    "WHERE owner_id=:owner"
                ),
                {"owner": owner},
            ).scalar_one()
            is None
        )
    # Loss of GitHub rights must not prevent owner logout/cancellation identity.
    assert sessions.resolve(first.session_token) is not None
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.browser_sessions "
                "SET created_at=clock_timestamp()-interval '8 days', "
                "expires_at=clock_timestamp()-interval '1 second' WHERE user_id=:owner"
            ),
            {"owner": owner},
        )
    assert sessions.resolve(first.session_token) is None
    second = sessions.login(github_id, "fixture-user", _TOKEN, 3600)
    assert second is not None
    sessions.revoke_account(owner)
    assert sessions.resolve(second.session_token) is None
    assert token_store.get_for_user(github_id) is None
    with t22_database.admin_engine.connect() as connection:
        material = connection.execute(
            text(
                "SELECT nonce,ciphertext FROM runtime_private.github_user_tokens "
                "WHERE owner_id=:owner"
            ),
            {"owner": owner},
        ).one()
    assert material == (None, None)


def test_oauth_state_expiry_is_rechecked_after_a_lock_wait(t22_database: T22Database) -> None:
    import time

    states = OAuthStateStore(t22_database.runtime_engine, _STATE_KEY)
    state = states.issue()
    digest = hashlib.sha256(state.state.encode()).digest()
    with t22_database.admin_engine.begin() as connection:
        expiry = connection.execute(
            text(
                "UPDATE runtime_private.github_oauth_states "
                "SET expires_at=clock_timestamp()+interval '1 second' "
                "WHERE state_hash=:hash RETURNING expires_at"
            ),
            {"hash": digest},
        ).scalar_one()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with t22_database.admin_engine.begin() as blocker:
            blocker.execute(
                text(
                    "SELECT state_hash FROM runtime_private.github_oauth_states "
                    "WHERE state_hash=:hash FOR UPDATE"
                ),
                {"hash": digest},
            ).scalar_one()
            future = pool.submit(states.consume, state.state, state.browser_binding)
            waited = False
            until = time.monotonic() + 0.7
            while time.monotonic() < until and not future.done():
                with t22_database.admin_engine.connect() as observer:
                    waited = observer.execute(
                        text(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                            "WHERE usename='continuum_runtime' "
                            "AND datname=current_database() AND wait_event_type='Lock' "
                            "AND query LIKE '%auth_consume_oauth_state%')"
                        )
                    ).scalar_one()
                if waited:
                    break
                time.sleep(0.01)
            assert waited
            blocker.execute(
                text(
                    "SELECT pg_sleep(GREATEST(0.0, EXTRACT(EPOCH FROM "
                    "CAST(:expiry AS timestamptz)-clock_timestamp()))+0.05)"
                ),
                {"expiry": expiry},
            )
        assert future.result(timeout=5) is None


def test_session_identity_wires_live_authorizer_into_review_and_denies_removed_rights(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    import httpx
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from test_approvals import _close_scenario, create_frozen_approval

    from continuum_api.auth.github import GitHubAuthorizationClient

    scenario = create_frozen_approval(t22_database, tmp_path)
    sessions, cipher = _services(t22_database)
    github_id = scenario.facts_provider.facts.github_user_id
    session = sessions.login(github_id, "fixture-user", _TOKEN, 3600)
    assert session is not None and session.principal.principal == scenario.principal
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    actor_writes = True
    requested = []
    repository_id = scenario.facts_provider.facts.repository_id

    def github(request: httpx.Request) -> httpx.Response:
        requested.append((request.method, request.url.path))
        path = request.url.path
        if path == "/user":
            payload = {"id": github_id}
        elif path == f"/repositories/{repository_id}":
            payload = {
                "id": repository_id,
                "private": False,
                "name": "project",
                "owner": {"login": "fixture"},
                "default_branch": "main",
                "permissions": {"push": actor_writes},
            }
        elif path == "/repos/fixture/project/installation":
            payload = {"id": 77}
        elif path == "/user/installations/77/repositories":
            payload = {"repositories": [{"id": repository_id}]}
        elif path == "/app/installations/77/access_tokens":
            permissions = json.loads(request.content)["permissions"]
            payload = {
                "token": "synthetic-installation-token",
                "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                "permissions": permissions,
                "repository_selection": "selected",
                "repositories": [{"id": repository_id}],
            }
        elif path == "/repos/fixture/project/git/ref/heads/main":
            payload = {"object": {"sha": "a" * 40}}
        else:
            return httpx.Response(404, json={})
        return httpx.Response(200, json=payload)

    boundary = GitHubAuthBoundary(
        engine=t22_database.runtime_engine,
        config=_config(),
        sessions=sessions,
        states=OAuthStateStore(t22_database.runtime_engine, _STATE_KEY),
        oauth=GitHubOAuthClient(_config()),
        limiter=OAuthRateLimiter(t22_database.runtime_engine, _STATE_KEY),
        storage=scenario.storage,
        authorization_factory=lambda principal: GitHubAuthorizationClient(
            app_id=77,
            app_private_key=SecretStr(pem.decode()),
            user_tokens=GitHubUserTokenStore(
                t22_database.runtime_engine, cipher, principal.account_id
            ),
            transport=httpx.MockTransport(github),
        ),
    )
    try:
        app = create_app(
            tmp_path, runtime_engine=t22_database.runtime_engine, github_auth_boundary=boundary
        )
        with TestClient(app, base_url=_ORIGIN) as client:
            client.cookies.set(SESSION_COOKIE, session.session_token)
            client.cookies.set(CSRF_COOKIE, session.csrf_token)
            review = client.get(f"/api/v1/runs/{scenario.run.run_id}/review")
            assert review.status_code == 200, review.text
            actor_writes = False
            result = client.post(
                f"/api/v1/approvals/{scenario.frozen_receipt.approval_id}/decision",
                headers={
                    "Origin": _ORIGIN,
                    "X-CSRF-Token": session.csrf_token,
                    "Idempotency-Key": "t29-removed-rights",
                },
                json={
                    "decision": "approve",
                    "manifest_sha256": scenario.frozen_receipt.manifest_sha256.hex(),
                },
            )
            assert result.status_code == 403
        with t22_database.admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT status FROM runtime_private.approvals WHERE approval_id=:approval"
                    ),
                    {"approval": scenario.frozen_receipt.approval_id},
                ).scalar_one()
                == "pending"
            )
        assert all(
            method == "GET" or path == "/app/installations/77/access_tokens"
            for method, path in requested
        )
    finally:
        _close_scenario(scenario)
