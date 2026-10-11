PYTHON_VERSION ?= 3.13

.PHONY: bootstrap dev check format-check lint typecheck test test-target build build-package build-container

bootstrap:
	uv sync --locked --python $(PYTHON_VERSION)

dev:
	uv run --locked --python $(PYTHON_VERSION) python -m continuum_api

check: format-check lint typecheck test

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

build: build-package build-container

build-package:
	uv build --out-dir dist

build-container:
	docker build --tag continuum-api:local .
