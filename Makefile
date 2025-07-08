SHELL := /bin/bash
PROJECT := graphsense-lib
VENV := venv
RELEASE := 'v25.07.3'
RELEASESEM := 'v2.5.3'

-include .env

all: format lint test build

tag-version:
	-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)

dev: install-dev
	 uv sync -e .[dev] --force-reinstall --all-extras
	 uv run pre-commit install

test:
	uv run --exact --all-extras pytest  -x -rx -vv -m "not slow" --cov=src --capture=no -W error

test-ci:
	uv run --exact --all-extras pytest  -x -rx -vv -m "not slow" --cov=src --capture=no -W error

test-with-base-dependencies-ci:
	uv run --exact --no-dev --group testing --extra swaps pytest  -x -rx -vv -m "not slow" --cov=src --capture=no


test-all:
	uv run --all-groups  pytest --cov=src -W error

install-dev:
	uv sync --all-packages --force-reinstall --all-extras

install:
	uv sync

lint:
	uv run ruff check tests src

format:
	uv run ruff check --select I --fix .
	uv run ruff format .

pre-commit:
	uv run --all-extras pre-commit run --all-files

build:
	uv build

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

.PHONY: all test install lint format build pre-commit docs test-all docs-latex publish tpublish tag-version click-bash-completion generate-tron-grpc-code test-with-base-dependencies-ci test-ci
