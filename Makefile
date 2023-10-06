SHELL := /bin/bash
PROJECT := graphsense-lib
VENV := venv
RELEASE := 'v23.09'
RELEASESEM := 'v1.8.1'

all: format lint test build

tag-version:
	-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)

dev:
	 pip install -e .[dev]
	 pre-commit install

test:
	pytest -v -m "not slow" --cov=src

test-all:
	pytest --cov=src

install-dev: dev
	pip install -e .

install:
	pip install .

lint:
	flake8 tests src

format:
	isort --profile black src
	black tests src

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

click-bash-completion:
	_GRAPHSENSE_CLI_COMPLETE=bash_source graphsense-cli

.PHONY: all test install lint format build pre-commit docs test-all docs-latex publish tpublish tag-version click-bash-completion
