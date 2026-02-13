SHELL := /bin/bash
PROJECT := graphsense-lib
VENV := venv
RELEASE := 'v25.11.19+devweb5'
RELEASESEM := 'v2.8.19+devweb5'

-include .env

gs_tagstore_db_url ?= 'postgresql+asyncpg://${POSTGRES_USER_TAGSTORE}:${POSTGRES_PASSWORD_TAGSTORE}@localhost:5432/tagstore'

all: format lint test build

tag-version:
	-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)

dev: install-dev
	 uv run pre-commit install

DANGEROUSLY_ACCELERATE_TESTS ?= 0

ifeq ($(DANGEROUSLY_ACCELERATE_TESTS),1)
PYTEST_OPTS := -x -rx -vv --capture=no -W error
PYTEST_MARK := -m "not slow"
else
PYTEST_OPTS := -x -rx -vv --cov=src --capture=no -W error --cov-report term-missing
PYTEST_MARK :=
endif

test: install-dev
	DANGEROUSLY_ACCELERATE_TESTS=$(DANGEROUSLY_ACCELERATE_TESTS) uv run --exact --all-extras pytest $(PYTEST_OPTS) $(PYTEST_MARK)

test-ci:
	uv run --exact --all-extras pytest  -x -rx -vv -m "not slow" --cov=src --capture=no -W error --cov-report term-missing

test-with-base-dependencies-ci:
	uv run --exact --no-dev --group testing --extra conversions --extra tagpacks --extra web pytest  -x -rx -vv -m "not slow" --cov=src --capture=no --cov-report term-missing


# Build pre-baked Cassandra image with all test schemas (resttest_* + pytest_*)
# This speeds up test startup significantly by avoiding runtime schema creation
build-fast-cassandra:
	docker build -t graphsense/cassandra-test:4.1.4 tests/web/cassandra/

# NOTE: REST regression tests live in tests/regressions/ (separate project with own Makefile)

install-dev:
	uv sync --dev --all-packages --force-reinstall --all-extras

install:
	uv sync

lint:
	uv run ruff check tests src

type-check:
	uv run --all-extras ty check

ty:
	uv run --all-extras ty check

format:
	uv run ruff check --fix .
	uv run ruff format .

pre-commit:
	uv run --all-extras pre-commit run --all-files

build:
	uv build

build-docker:
	docker build -t graphsense-lib .

version:
	uv run python -m setuptools_scm

generate-tron-grpc-code:
	uv run python -m grpc_tools.protoc\
		--python_out=./src/\
		--grpc_python_out=./src/\
		--proto_path=./src/\
		./src/graphsenselib/ingest/tron/grpc/api/tron_api.proto
	uv run python -m grpc_tools.protoc\
		--python_out=./src/\
		--proto_path=./src/\
		./src/graphsenselib/ingest/tron/grpc/core/*.proto

click-bash-completion:
	_GRAPHSENSE_CLI_COMPLETE=bash_source graphsense-cli

serve-tagstore:
	@gs_tagstore_db_url=${gs_tagstore_db_url} uv run uvicorn --reload --log-level debug src.graphsenselib.tagstore.web.main:app

# REST API server
GS_REST_DEV_PORT ?= 9000

serve-web:
	uv run --extra web uvicorn graphsenselib.web.app:create_app --factory --host localhost --port ${GS_REST_DEV_PORT} --reload

# Python client generation (requires running server on port 9000)
run-codegen: generate-python-client

generate-python-client:
	cd clients/python && make generate-openapi-client

# API version management
update-api-version:
	@version=$$(echo $(RELEASESEM) | sed "s/^['\"]\\?v\\?//" | sed "s/['\"]$$//"); \
	sed -i 's/__api_version__ = .*/__api_version__ = "'$$version'"/' src/graphsenselib/web/version.py; \
	echo "Updated API version to $$version"

check-api-version:
	@version=$$(echo $(RELEASESEM) | sed "s/^['\"]\\?v\\?//" | sed "s/['\"]$$//"); \
	file_version=$$(grep -oP '__api_version__ = "\K[^"]+' src/graphsenselib/web/version.py); \
	if [ "$$version" != "$$file_version" ]; then \
		echo "Version mismatch: Makefile has $$version, version.py has $$file_version"; \
		echo "Run 'make update-api-version' to fix"; \
		exit 1; \
	else \
		echo "API version aligned: $$version"; \
	fi

# Docker targets for REST API
serve-docker:
	docker run --rm -it --network='host' -e NUM_THREADS=1 -e NUM_WORKERS=1 -v "${PWD}/instance/config.yaml:/srv/graphsense-rest/instance/config.yaml:Z" graphsense-lib:latest

package-ui:
	- rm -rf tagpack/admin-ui/dist
	cd tagpack/admin-ui; npm install && npx --yes elm-land build && cp dist/assets/index-*.js ../../src/graphsenselib/tagstore/web/statics/assets/index.js

# NOTE: Tagpack integration tests have moved to iknaio-tests-nightly repository
# Run: cd ../iknaio/iknaio-tests-nightly && make test-tagpack

.PHONY: all test install lint format build pre-commit test-all type-check ty-check tag-version click-bash-completion generate-tron-grpc-code test-with-base-dependencies-ci test-ci serve-tagstore serve-web run-codegen generate-python-client serve-docker package-ui build-fast-cassandra update-api-version check-api-version
