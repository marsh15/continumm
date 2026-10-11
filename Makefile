PYTHON_VERSION ?= 3.13

.PHONY: bootstrap dev dev-web check format-check lint typecheck test test-target test-ui openapi frontend-install frontend-openapi-check build build-package build-frontend build-container

bootstrap:
	uv sync --locked --python $(PYTHON_VERSION)
	cd frontend && npm ci

dev:
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api

dev-web:
	cd frontend && npm run dev

check: format-check lint typecheck test test-ui

format-check:
	uv run --locked --python $(PYTHON_VERSION) ruff format --check src tests

lint:
	uv run --locked --python $(PYTHON_VERSION) ruff check src tests

typecheck:
	uv run --locked --python $(PYTHON_VERSION) mypy

test:
	uv run --locked --python $(PYTHON_VERSION) pytest -q

test-target:
	@test -n "$(TARGET)" || (echo "Usage: make test-target TARGET=tests/path.py" >&2; exit 2)
	uv run --locked --python $(PYTHON_VERSION) pytest -q "$(TARGET)"

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

build: build-package build-frontend build-container

build-package:
	uv build --out-dir dist

build-frontend: frontend-openapi-check
	cd frontend && npm run build

build-container:
	docker build --tag continuum-api:local .
