PYTHON_VERSION ?= 3.13

.PHONY: bootstrap dev dev-web check format-check lint typecheck test test-target test-integration test-edge edge-check edge-format-check migrate-up migrate-down smoke-hosted smoke-sandbox sandbox-crash-fixture sandbox-observe sandbox-cleanup test-ui test-e2e openapi frontend-install frontend-openapi-check edge-install build build-package build-frontend build-container eval-live

bootstrap:
	uv sync --locked --python $(PYTHON_VERSION)
	cd frontend && npm ci
	$(MAKE) edge-install

dev:
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api

dev-web:
	cd frontend && npm run dev

check: format-check lint typecheck test test-ui edge-format-check edge-check test-edge

format-check:
	uv run --locked --python $(PYTHON_VERSION) ruff format --check src tests migrations

lint:
	uv run --locked --python $(PYTHON_VERSION) ruff check src tests migrations

typecheck:
	uv run --locked --python $(PYTHON_VERSION) mypy

test:
	uv run --locked --python $(PYTHON_VERSION) python scripts/run_tests.py -q

test-target:
	@test -n "$(TARGET)" || (echo "Usage: make test-target TARGET=tests/path.py" >&2; exit 2)
	uv run --locked --python $(PYTHON_VERSION) python scripts/run_tests.py -q "$(TARGET)"

eval-live:
	@test "$(PROVIDER)" = "openai" -o "$(PROVIDER)" = "anthropic" || (echo "Usage: make eval-live PROVIDER=openai|anthropic" >&2; exit 2)
	uv run --locked --python $(PYTHON_VERSION) python scripts/eval_live.py --provider "$(PROVIDER)"

test-integration:
	uv run --locked --python $(PYTHON_VERSION) python scripts/run_tests.py -q tests/integration

test-edge:
	npm --prefix tools/edge test

edge-check:
	npm --prefix tools/edge run check

edge-format-check:
	npm --prefix tools/edge exec -- deno fmt --check --config supabase/functions/wake-dispatch/deno.json supabase/functions/github-ingress supabase/functions/wake-dispatch

edge-install:
	npm ci --prefix tools/edge

migrate-up:
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api.migrations upgrade head

migrate-down:
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api.migrations downgrade base

smoke-hosted:
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api.connectivity.smoke

smoke-sandbox:
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api.sandbox.smoke lifecycle

sandbox-crash-fixture:
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api.sandbox.smoke crash-fixture

sandbox-observe:
	@test -n "$(NAME)" || (echo "Usage: make sandbox-observe NAME=continuum-t04-<uuid> [PROCESS_ID=<id>]" >&2; exit 2)
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api.sandbox.smoke observe --name "$(NAME)" $(if $(PROCESS_ID),--process-id "$(PROCESS_ID)",)

sandbox-cleanup:
	@test -n "$(NAME)" || (echo "Usage: make sandbox-cleanup NAME=continuum-t04-<uuid>" >&2; exit 2)
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api.sandbox.smoke cleanup --name "$(NAME)"

frontend-install:
	cd frontend && npm ci

openapi:
	uv run --locked --python $(PYTHON_VERSION) python scripts/export_openapi.py
	cd frontend && npm run generate:api

frontend-openapi-check: frontend-install
	uv run --locked --python $(PYTHON_VERSION) python scripts/export_openapi.py --check
	cd frontend && npm run check:api

test-ui: frontend-openapi-check
	cd frontend && npm run typecheck && npm run test -- --run

test-e2e: build-frontend
	cd frontend && npm run test:e2e

build: build-package build-frontend build-container

build-package:
	uv build --out-dir dist

build-frontend: frontend-openapi-check
	cd frontend && npm run build

build-container:
	docker build --tag continuum-api:local .
