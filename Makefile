SHELL := /bin/bash
PROJECT := graphsense-lib
VENV := venv
RELEASE := 'v25.03.2'
RELEASESEM := 'v2.4.10'

-include .env

all: format lint test build

tag-version:
	-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)

dev:
	 pip install -e .[dev] --force-reinstall --upgrade
	 pre-commit install

test:
	pytest -v -m "not slow" --cov=src -W error

test-all:
	pytest --cov=src -W error

install-dev:
	pip install -e .[dev] --force-reinstall --upgrade

install:
	pip install .

lint:
	ruff check tests src

format:
	ruff check --select I --fix .
	ruff format .

docs:
	tox -e docs

docs-latex:
	tox -e docs-latex

pre-commit:
	pre-commit run --all-files

build:
	tox -e clean
	tox -e build

tpublish: build version
	tox -e publish

publish: build version
	tox -e publish -- --repository pypi

version:
	python -m setuptools_scm

generate-tron-grpc-code:
	python -m grpc_tools.protoc\
		--python_out=./src/\
		--grpc_python_out=./src/\
		--proto_path=./src/\
		./src/graphsenselib/ingest/tron/grpc/api/tron_api.proto
	python -m grpc_tools.protoc\
		--python_out=./src/\
		--proto_path=./src/\
		./src/graphsenselib/ingest/tron/grpc/core/*.proto

click-bash-completion:
	_GRAPHSENSE_CLI_COMPLETE=bash_source graphsense-cli

.PHONY: all test install lint format build pre-commit docs test-all docs-latex publish tpublish tag-version click-bash-completion generate-tron-grpc-code
