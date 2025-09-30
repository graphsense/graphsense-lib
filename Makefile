SHELL := /bin/bash
PROJECT := graphsense-lib
VENV := venv
RELEASE := 'v25.09.4'
RELEASESEM := 'v2.7.4'

-include .env

gs_tagstore_db_url ?= 'postgresql+asyncpg://${POSTGRES_USER_TAGSTORE}:${POSTGRES_PASSWORD_TAGSTORE}@localhost:5432/tagstore'

all: format lint test build

tag-version:
	-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)

dev: install-dev
	 uv run pre-commit install

test: install-dev
	uv run --exact --all-extras pytest  -x -rx -vv -m "not slow" --cov=src --capture=no -W error --cov-report term-missing

test-ci:
	uv run --exact --all-extras pytest  -x -rx -vv -m "not slow" --cov=src --capture=no -W error --cov-report term-missing

test-with-base-dependencies-ci:
	uv run --exact --no-dev --group testing --extra conversions pytest  -x -rx -vv -m "not slow" --cov=src --capture=no --cov-report term-missing


test-all:
	uv run --all-groups  pytest --cov=src -W error --cov-report term-missing

install-dev:
	uv sync --all-packages --force-reinstall --all-extras

install:
	uv sync

lint:
	uv run ruff check tests src

type-check:
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

package-ui:
	- rm -rf tagpack/admin-ui/dist
	cd tagpack/admin-ui; npm install && npx --yes elm-land build && cp dist/assets/index-*.js ../../src/graphsenselib/tagstore/web/statics/assets/index.js

.PHONY: all test install lint format build pre-commit test-all type-check tag-version click-bash-completion generate-tron-grpc-code test-with-base-dependencies-ci test-ci serve-tagstore package-ui
