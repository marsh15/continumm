# Continumm

Continumm is a service under development for turning bounded coding tasks into reviewed GitHub pull requests. The current implementation establishes its Python API and React frontend foundations. The public service has not launched.

## Local development

The backend requires Python 3.13 and [uv](https://docs.astral.sh/uv/); the frontend requires Node.js 22.12 or newer. Install the locked dependencies with:

```sh
make bootstrap
```

Run the API and Vite frontend in separate terminals:

```sh
make dev
```

```sh
make dev-web
```

The API binds to `127.0.0.1:8000`. `GET /health/live` reports process liveness. `GET /health/ready` currently returns `503 not_ready` until a database connection check is implemented; it does not imply database availability. The frontend displays these actual API responses and identifies the unfinished readiness check.

The frontend build direction and generated OpenAPI workflow are documented in [docs/frontend.md](docs/frontend.md). Run all backend checks and frontend component tests with:

```sh
make test-target TARGET=tests/unit/test_settings.py
make check
make build
```

`make build` creates a Python distribution under `dist/`, a static frontend build, and the local Docker image `continuum-api:local`. The container serves the built frontend from the API process. The image defaults to production configuration and exits with a redacted configuration error until explicit deployment settings are provided. Authentication and persistence are not implemented yet, so this foundation is not a public service.

## Configuration

Settings use the `CONTINUUM_` environment prefix. Local development defaults are loopback-only. Development and test modes reject non-loopback hosts. Production requires an explicit bind host, database URL, and encryption key. Settings values and request data are not written to access logs; application logs use JSON and redact common credential forms.

## Hosting preparation

The local T03 adapters and guarded startup probe prepare a controlled Render/Supabase
connectivity check. The hosted projects are not set up, so the hosted proof remains
pending. Follow [hosting setup](docs/hosting-setup.md) and retain results in the
[evidence report](docs/evidence/t03-hosted.md). The probe is disabled by default;
`make smoke-hosted` refuses a normal local process.

The complete product scope and ordered implementation backlog are in [tasks/plan.md](tasks/plan.md) and [tasks/todo.md](tasks/todo.md). Verified work and pending gates are recorded in [tasks/progress.md](tasks/progress.md).
