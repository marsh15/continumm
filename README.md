# Continuumm

Continumm is a planning-stage service for turning bounded coding tasks into reviewed GitHub pull requests. The current implementation establishes the Python API foundation described in [the implementation plan](tasks/plan.md).

## Backend setup

The backend requires Python 3.13 and [uv](https://docs.astral.sh/uv/). Install the locked development dependencies and run the local API with:

```sh
make bootstrap
make dev
```

The local server binds to `127.0.0.1:8000`. `GET /health/live` reports process liveness. `GET /health/ready` currently returns `503 not_ready` until a database connection check is implemented; it does not imply database availability.

Run the settings unit tests, backend checks, and package/container build with:

```sh
make test-target TARGET=tests/unit/test_settings.py
make check
make build
```

`make build` creates a Python distribution under `dist/` and the local Docker image `continuum-api:local`. The image defaults to production configuration and exits with a redacted configuration error until explicit deployment settings are provided. Do not publish this skeleton as a public service: authentication and persistence are not implemented yet.

## Configuration

Settings use the `CONTINUUM_` environment prefix. Local development defaults are loopback-only. Development and test modes reject non-loopback hosts. Production requires an explicit bind host, database URL, and encryption key. Settings values and request data are not written to access logs; application logs use JSON and redact common credential forms.

The complete product scope and ordered implementation backlog are in [tasks/plan.md](tasks/plan.md) and [tasks/todo.md](tasks/todo.md).
