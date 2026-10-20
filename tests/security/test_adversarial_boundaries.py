"""Adversarial input cannot add tool authority or weaken browser boundaries."""

from __future__ import annotations

import io
import tarfile

import pytest
from fastapi.testclient import TestClient

from continuum_api.app import create_app
from continuum_api.artifacts.archives import ArchiveError, verify_archive
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext, authorize_tool_call


@pytest.mark.parametrize("path", ["../outside", "/etc/passwd", "a/../../escape", "a\\..\\escape"])
def test_archive_paths_cannot_escape_the_source_boundary(path: str) -> None:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        entry = tarfile.TarInfo(path)
        entry.size = 1
        archive.addfile(entry, io.BytesIO(b"x"))
    with pytest.raises(ArchiveError):
        verify_archive(output.getvalue())


@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE])
def test_archive_links_are_not_restored(kind: bytes) -> None:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        entry = tarfile.TarInfo("src/link")
        entry.type = kind
        entry.linkname = "../../outside"
        archive.addfile(entry)
    with pytest.raises(ArchiveError):
        verify_archive(output.getvalue())


@pytest.mark.parametrize(
    "call",
    [
        {"name": "read_file", "path": ".env"},
        {"name": "read_file", "path": ".ssh/id_rsa"},
        {"name": "read_file", "path": "../credentials"},
        {"name": "get_secret", "key": "OPENAI_API_KEY"},
        {"name": "shell", "command": "printenv"},
        {"name": "publish", "approved": True},
        {"name": "http_request", "url": "https://attacker.invalid"},
        {"name": "read_file", "path": "src/app.py", "actor_can_write_repository": True},
        {
            "name": "apply_patch",
            "edits": [
                {
                    "path": "tests/test_original.py",
                    "expected_text": "assert False",
                    "replacement_text": "assert True",
                }
            ],
        },
    ],
)
def test_injected_instructions_do_not_grant_secrets_execution_or_publication(call: object) -> None:
    paths = frozenset(
        {"src/app.py", "tests/test_original.py", "pyproject.toml", ".env", ".ssh/id_rsa"}
    )
    context = ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id="python-pytest",
        profile_version="1",
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256="a" * 64,
        baseline_source_paths=paths,
        baseline_test_paths=frozenset({"tests/test_original.py", "pyproject.toml"}),
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )
    decision = authorize_tool_call(call, context, paths, {path: 10 for path in paths})
    assert not decision.allowed


def test_browser_policy_cannot_be_relaxed_by_request_headers() -> None:
    with TestClient(create_app(), base_url="https://service.example") as client:
        response = client.get("/health/live", headers={"Content-Security-Policy": "default-src *"})
    policy = response.headers["content-security-policy"]
    assert "script-src 'self'" in policy and "'unsafe-inline'" not in policy
    assert "object-src 'none'" in policy and "frame-ancestors 'none'" in policy
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert "max-age=" in response.headers["strict-transport-security"]
