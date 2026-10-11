"""Static frontend serving stays inside the built asset directory."""

from __future__ import annotations

from fastapi import HTTPException
from fastapi.testclient import TestClient

from continuum_api.app import create_app


def add_route_before_static_fallback(application, path: str, endpoint) -> None:
    fallback = application.router.routes.pop()
    application.add_api_route(path, endpoint, methods=["GET"])
    application.router.routes.append(fallback)


def test_spa_serves_only_built_files_and_extensionless_browser_routes(tmp_path) -> None:
    static_dir = tmp_path / "dist"
    assets = static_dir / "assets"
    assets.mkdir(parents=True)
    (static_dir / "index.html").write_text("<!doctype html><title>Continumm</title>")
    (assets / "app.js").write_text("console.log('built asset')")
    client = TestClient(create_app(static_dir))

    page = client.get("/settings", headers={"accept": "text/html"})
    asset = client.get("/assets/app.js")

    assert page.status_code == 200
    assert page.text == "<!doctype html><title>Continumm</title>"
    assert page.headers["cache-control"] == "no-cache"
    assert asset.status_code == 200
    assert asset.text == "console.log('built asset')"
    assert asset.headers["cache-control"] == "public, max-age=31536000, immutable"


def test_missing_api_and_health_routes_return_json_errors_not_the_spa(tmp_path) -> None:
    static_dir = tmp_path / "dist"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("frontend shell")
    client = TestClient(create_app(static_dir))

    for path in ("/api/v1/not-implemented", "/health/not-implemented", "/auth/not-implemented"):
        response = client.get(path, headers={"accept": "text/html"})

        assert response.status_code == 404
        assert response.headers["content-type"].startswith("application/json")
        assert response.json()["code"] == "not_found"
        assert response.json()["request_id"] == response.headers["x-request-id"]
        assert "frontend shell" not in response.text


def test_static_files_and_spa_index_cannot_follow_symlinks_outside_dist(tmp_path) -> None:
    static_dir = tmp_path / "dist"
    static_dir.mkdir()
    outside_file = tmp_path / "secret.txt"
    outside_file.write_text("private source")
    (static_dir / "escape.txt").symlink_to(outside_file)
    client = TestClient(create_app(static_dir))

    asset_response = client.get("/escape.txt")
    assert asset_response.status_code == 404
    assert "private source" not in asset_response.text

    (static_dir / "index.html").symlink_to(outside_file)
    index_response = client.get("/", headers={"accept": "text/html"})
    assert index_response.status_code == 404
    assert "private source" not in index_response.text


def test_unhandled_errors_use_the_envelope_and_keep_request_id_header(tmp_path) -> None:
    application = create_app(tmp_path)

    async def crash() -> None:
        raise RuntimeError("private exception details")

    add_route_before_static_fallback(application, "/api/test-crash", crash)

    client = TestClient(application, raise_server_exceptions=False)
    response = client.get("/api/test-crash")

    assert response.status_code == 500
    assert response.json() == {
        "code": "internal_error",
        "message": "The service could not complete this request.",
        "retryable": True,
        "request_id": response.headers["x-request-id"],
    }
    assert "private exception details" not in response.text


def test_http_error_preserves_protocol_headers(tmp_path) -> None:
    application = create_app(tmp_path)

    async def auth_required() -> None:
        raise HTTPException(
            status_code=401, detail="Sign-in required", headers={"WWW-Authenticate": "Bearer"}
        )

    add_route_before_static_fallback(application, "/api/test-auth", auth_required)

    response = TestClient(application).get("/api/test-auth")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json()["request_id"] == response.headers["x-request-id"]
