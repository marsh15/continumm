"""Small fail-closed checks for the external-attention browser boundary."""

from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from continuum_api.app import create_app
from continuum_api.runtime.external_attention_routes import _safe_pr_url


def test_external_attention_route_requires_authenticated_principal(tmp_path: Path) -> None:
    def reject(_request: object) -> None:
        raise HTTPException(status_code=401, detail="authentication_required")

    engine = create_engine("sqlite://")
    try:
        application = create_app(
            tmp_path,
            principal_resolver=reject,
            runtime_engine=engine,
            external_attention_service_factory=None,
        )
        with TestClient(application) as client:
            response = client.get(
                "/api/v1/runs/00000000-0000-0000-0000-000000000001/external-effects"
            )
        assert response.status_code == 401
    finally:
        engine.dispose()


def test_pr_evidence_url_requires_exact_github_host_and_path() -> None:
    assert _safe_pr_url("https://github.com/example/repo/pull/17", 17)
    assert not _safe_pr_url("https://github.com.attacker.test/example/repo/pull/17", 17)
    assert not _safe_pr_url("https://github.com/example/repo/pull/17?next=elsewhere", 17)
    assert not _safe_pr_url("https://github.com/example/repo/pull/18", 17)
