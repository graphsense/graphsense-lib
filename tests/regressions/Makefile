SHELL := /bin/bash

# ============================================================================
# Shared defaults
# ============================================================================

GSLIB_REPO := https://github.com/graphsense/graphsense-lib.git
GSLIB_PATH := $(shell cd ../.. && pwd)

# Override to restrict all suites to specific currencies (comma-separated).
# Per-suite overrides (e.g., CASSANDRA_CURRENCIES) still take precedence.
CURRENCIES ?= btc,eth,ltc,bch,zec,trx

# Reference version for cross-version tests (cassandra, deltalake)
REF_VERSION ?= v25.11.18

# ============================================================================
# Help (default target)
# ============================================================================

.DEFAULT_GOAL := help

help: ## Show this help
	@echo "Usage: make <target> [VAR=value ...]"
	@echo ""
	@echo "Global variables:"
	@echo "  CURRENCIES=btc,eth      Restrict all suites to specific currencies"
	@echo "  REF_VERSION=v25.11.18   Reference version for cross-version tests"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Setup
# ============================================================================

install: ## Install test dependencies
	uv sync

lint: ## Lint regression test code
	uv run ruff check tests/ --exclude .venv

format: ## Auto-format regression test code
	uv run ruff check --fix tests/ --exclude .venv
	uv run ruff format tests/ --exclude .venv

# ============================================================================
# Quick smoke test (single currency, all ingest suites)
# ============================================================================

QUICK_CURRENCY ?= eth

test-quick: ## Run all ingest suites for a single currency (default: eth)
	@echo "========================================================================"
	@echo "  Quick smoke test: $(QUICK_CURRENCY)"
	@echo "========================================================================"
	@ret=0; \
	$(MAKE) test-cassandra CASSANDRA_CURRENCIES=$(QUICK_CURRENCY) || ret=1; \
	$(MAKE) test-deltalake DELTA_CURRENCIES=$(QUICK_CURRENCY) || ret=1; \
	$(MAKE) test-sink-consistency SINK_CONSISTENCY_CURRENCIES=$(QUICK_CURRENCY) || ret=1; \
	$(MAKE) test-continuation CONTINUATION_CURRENCIES=$(QUICK_CURRENCY) || ret=1; \
	$(MAKE) test-transformation TRANSFORMATION_CURRENCIES=$(QUICK_CURRENCY) || ret=1; \
	exit $$ret

# ============================================================================
# Delta Lake cross-version compatibility tests
# ============================================================================

DELTA_REF_VERSION ?= $(REF_VERSION)
DELTA_CURRENCIES ?= $(CURRENCIES)
DELTA_RANGE_CATEGORIES ?= mid
DELTA_PERF_RANGE_CATEGORIES ?= mid

test-deltalake: ## Delta Lake cross-version compatibility
	DELTA_REF_VERSION=$(DELTA_REF_VERSION) \
	DELTA_CURRENCIES=$(DELTA_CURRENCIES) \
	DELTA_RANGE_CATEGORIES=$(DELTA_RANGE_CATEGORIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/deltalake/ -v -s -m deltalake

test-deltalake-comprehensive: ## Delta Lake — all range categories
	DELTA_REF_VERSION=$(DELTA_REF_VERSION) \
	DELTA_CURRENCIES=$(DELTA_CURRENCIES) \
	DELTA_RANGE_CATEGORIES=genesis,old,mid,new,protocol \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/deltalake/ -v -s -m deltalake

test-deltalake-perf: ## Delta Lake ingest performance
	DELTA_REF_VERSION=$(DELTA_REF_VERSION) \
	DELTA_CURRENCIES=$(DELTA_CURRENCIES) \
	DELTA_RANGE_CATEGORIES=$(DELTA_PERF_RANGE_CATEGORIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/deltalake/test_performance.py -v -s -m deltalake_perf

test-deltalake-all: ## Delta Lake compatibility + performance
	@echo "Running Delta Lake compatibility + performance regression suite..."
	@$(MAKE) test-deltalake
	@$(MAKE) test-deltalake-perf

# ============================================================================
# Cassandra ingest regression tests
# ============================================================================

CASSANDRA_REF_VERSION ?= $(REF_VERSION)
CASSANDRA_CURRENCIES ?= $(CURRENCIES)

test-cassandra: ## Cassandra cross-version ingest
	CASSANDRA_REF_VERSION=$(CASSANDRA_REF_VERSION) \
	CASSANDRA_CURRENCIES=$(CASSANDRA_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/cassandra/ -v -s -m cassandra

# ============================================================================
# Sink consistency tests (individual vs dual-sink)
# ============================================================================

SINK_CONSISTENCY_CURRENCIES ?= $(CURRENCIES)

test-sink-consistency: ## Sink consistency (dual-sink vs individual)
	SINK_CONSISTENCY_CURRENCIES=$(SINK_CONSISTENCY_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/sink_consistency/ -v -s -m sink_consistency

# ============================================================================
# Continuation (split-ingest) tests
# ============================================================================

CONTINUATION_CURRENCIES ?= $(CURRENCIES)

test-continuation: ## Continuation (split-ingest vs one-shot)
	CONTINUATION_CURRENCIES=$(CONTINUATION_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/continuation/ -v -s -m continuation

# ============================================================================
# Transformation tests (Delta Lake → PySpark → Cassandra equivalence)
# ============================================================================

TRANSFORMATION_CURRENCIES ?= $(CURRENCIES)

test-transformation: ## Transformation (Delta → PySpark → Cassandra)
	TRANSFORMATION_CURRENCIES=$(TRANSFORMATION_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/transformation/ -v -s -m transformation

# ============================================================================
# Combined suites
# ============================================================================

test-ingest: ## Run all ingest regression tests
	@echo "========================================================================"
	@echo "  Running all ingest regression tests"
	@echo "========================================================================"
	@ret=0; \
	$(MAKE) test-cassandra || ret=1; \
	$(MAKE) test-deltalake || ret=1; \
	$(MAKE) test-sink-consistency || ret=1; \
	$(MAKE) test-continuation || ret=1; \
	$(MAKE) test-transformation || ret=1; \
	exit $$ret

# ============================================================================
# REST API regression tests
# ============================================================================

# Baseline version: set to a tag, branch, or commit hash
REST_BASELINE ?=

# Branch to test against baseline
REST_CURRENT_BRANCH ?= master

# Optional: local graphsense-lib path (for rest-build-local)
GSLIB ?=

# Ports (high ports to avoid conflicts)
REST_CURRENT_PORT ?= 19100
REST_BASELINE_PORT ?= 19101

# Docker image names
IMAGE_CURRENT := gslib-test:current
IMAGE_BASELINE := gslib-test:baseline

# Config (graphsense-REST format, mounted into container)
REST_CONFIG_FILE ?= $(PWD)/../../instance/config.yaml

rest-build-local: ## Build current REST image from local path (GSLIB=)
	@if [ -z "$(GSLIB)" ]; then \
		echo "Error: GSLIB not set. Usage: make rest-build-local GSLIB=/path/to/graphsense-lib"; \
		exit 1; \
	fi
	@echo "Building current from local: $(GSLIB)"
	docker build -t $(IMAGE_CURRENT) $(GSLIB)

rest-build-develop: ## Build current REST image from branch
	@echo "Building current from $(REST_CURRENT_BRANCH) branch..."
	@rm -rf /tmp/gslib-current && \
	git clone --depth 1 --branch $(REST_CURRENT_BRANCH) $(GSLIB_REPO) /tmp/gslib-current && \
	docker build -t $(IMAGE_CURRENT) /tmp/gslib-current

rest-build-baseline: ## Build baseline REST image (REST_BASELINE=)
	@if [ -z "$(REST_BASELINE)" ]; then \
		echo "Error: REST_BASELINE not set. Usage: make rest-build-baseline REST_BASELINE=v26.01.0"; \
		exit 1; \
	fi
	@echo "Building baseline: $(REST_BASELINE)"
	@rm -rf /tmp/gslib-baseline && \
	git clone $(GSLIB_REPO) /tmp/gslib-baseline && \
	cd /tmp/gslib-baseline && git checkout $(REST_BASELINE) && \
	cd /tmp/gslib-baseline && sed -i 's/"pydantic>=2.0.0"/"pydantic>=2.0.0",\n    "pydantic-settings>=2.0.0,<2.13.0"/' pyproject.toml && uv lock && \
	docker build -t $(IMAGE_BASELINE) /tmp/gslib-baseline

rest-ensure-baseline:
	@if ! docker image inspect $(IMAGE_BASELINE) >/dev/null 2>&1; then \
		if [ -z "$(REST_BASELINE)" ]; then \
			echo "Error: REST_BASELINE not set and no baseline image exists."; \
			echo "Usage: make test-rest-local REST_BASELINE=v26.01.0 GSLIB=/path/to/repo"; \
			exit 1; \
		fi; \
		$(MAKE) rest-build-baseline; \
	fi

rest-ensure-servers: rest-ensure-baseline
	@if [ ! -f "$(REST_CONFIG_FILE)" ] && [ ! -L "$(REST_CONFIG_FILE)" ]; then \
		echo "Error: REST_CONFIG_FILE not found at $(REST_CONFIG_FILE)"; \
		exit 1; \
	fi
	@REAL_CONFIG=$$(readlink -f "$(REST_CONFIG_FILE)") && \
	if ! docker ps --format '{{.Names}}' | grep -q '^gs-current$$'; then \
		echo "Starting current server on port $(REST_CURRENT_PORT)..."; \
		docker run -d --name gs-current --rm --network=host \
			-v "$$REAL_CONFIG:/srv/graphsense-rest/instance/config.yaml:ro" \
			-e NUM_WORKERS=1 -e NUM_THREADS=1 \
			$(IMAGE_CURRENT) \
			sh -c 'gunicorn -c /opt/gunicorn-conf.py --bind 0.0.0.0:$(REST_CURRENT_PORT) "graphsenselib.web.app:create_app(\"$$CONFIG_FILE\")" --worker-class uvicorn.workers.UvicornWorker'; \
	fi && \
	if ! docker ps --format '{{.Names}}' | grep -q '^gs-baseline$$'; then \
		echo "Starting baseline server on port $(REST_BASELINE_PORT)..."; \
		docker run -d --name gs-baseline --rm --network=host \
			-v "$$REAL_CONFIG:/config.yaml:ro" \
			-e CONFIG_FILE=/config.yaml -e NUM_WORKERS=1 -e NUM_THREADS=1 \
			$(IMAGE_BASELINE) \
			sh -c 'gunicorn -c /opt/gunicorn-conf.py --bind 0.0.0.0:$(REST_BASELINE_PORT) "graphsenselib.web.app:create_app(\"$$CONFIG_FILE\")" --worker-class uvicorn.workers.UvicornWorker'; \
	fi
	@echo "Waiting for servers to be ready..."
	@ready=0; \
	for i in $$(seq 1 60); do \
		if curl -s http://localhost:$(REST_CURRENT_PORT)/stats >/dev/null 2>&1 && \
		   curl -s http://localhost:$(REST_BASELINE_PORT)/stats >/dev/null 2>&1; then \
			ready=1; break; \
		fi; \
		sleep 2; \
	done; \
	if [ "$$ready" -ne 1 ]; then \
		echo "Error: servers did not become ready in time."; \
		echo "--- current logs ---"; docker logs gs-current 2>&1 | tail -20; \
		echo "--- baseline logs ---"; docker logs gs-baseline 2>&1 | tail -20; \
		$(MAKE) rest-stop; \
		exit 1; \
	fi
	@echo "Current:  http://localhost:$(REST_CURRENT_PORT)"
	@echo "Baseline: http://localhost:$(REST_BASELINE_PORT)"

rest-serve: rest-ensure-servers ## Start both REST servers

rest-stop: ## Stop REST server containers
	-docker stop gs-current gs-baseline 2>/dev/null || true

test-rest-local: rest-build-local rest-build-baseline rest-ensure-servers ## Full REST regression (local build)
	@$(MAKE) rest-run-tests; ret=$$?; $(MAKE) rest-stop; exit $$ret

test-rest-dev-branch: rest-build-develop rest-build-baseline rest-ensure-servers ## Full REST regression (branch build)
	@$(MAKE) rest-run-tests; ret=$$?; $(MAKE) rest-stop; exit $$ret

rest-info:
	@echo "========================================================================"
	@echo "  iknaio-tests-nightly — REST API"
	@echo "========================================================================"
	@echo "  Date:             $$(date -Iseconds)"
	@echo "  Host:             $$(hostname)"
	@echo "  Current branch:   $(REST_CURRENT_BRANCH)"
	@echo "  Baseline ref:     $(REST_BASELINE)"
	@echo "  Current image:    $(IMAGE_CURRENT) ($$(docker inspect --format='{{.Id}}' $(IMAGE_CURRENT) 2>/dev/null | cut -c8-19 || echo 'n/a'))"
	@echo "  Baseline image:   $(IMAGE_BASELINE) ($$(docker inspect --format='{{.Id}}' $(IMAGE_BASELINE) 2>/dev/null | cut -c8-19 || echo 'n/a'))"
	@echo "  Current server:   http://localhost:$(REST_CURRENT_PORT)"
	@echo "  Baseline server:  http://localhost:$(REST_BASELINE_PORT)"
	@echo "  Config file:      $(REST_CONFIG_FILE)"
	@echo "========================================================================"

rest-run-tests: rest-info
	@ret=0; \
	$(MAKE) test-rest-fuzz || ret=1; \
	$(MAKE) test-rest-manual || ret=1; \
	$(MAKE) test-rest-loki || ret=1; \
	exit $$ret

test-rest-fuzz: rest-ensure-servers ## REST fuzz regression tests
	CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
	uv run pytest tests/rest/test_baseline_regression.py -v -m regression

test-rest-manual: rest-ensure-servers ## REST manual regression tests
	CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
	uv run pytest tests/rest/test_manual_regression.py -v -m regression

test-rest-loki: rest-ensure-servers ## REST Loki-generated tests
	@if [ -f tests/rest/test_loki_generated.py ]; then \
		CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
		BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
		uv run pytest tests/rest/test_loki_generated.py -v -m loki_generated; \
	else \
		echo "Skipping loki tests (run 'make generate-loki' to create)"; \
	fi

generate-loki: ## Generate Loki tests from production logs (LOKI_URL=)
	uv run scripts/generate_loki_tests.py

# ============================================================================
# Cleanup
# ============================================================================

rest-clean: rest-stop
	-docker rmi $(IMAGE_CURRENT) $(IMAGE_BASELINE) 2>/dev/null || true
	-rm -rf /tmp/gslib-current /tmp/gslib-baseline

clean-venvs: ## Remove cached test virtual environments
	rm -rf /tmp/gslib-deltalake-testvenvs

clean: rest-clean clean-venvs ## Remove all test artifacts

# ============================================================================
# .PHONY
# ============================================================================

.PHONY: help install lint format \
	test-quick test-ingest \
	test-deltalake test-deltalake-comprehensive test-deltalake-perf test-deltalake-all \
	test-cassandra test-sink-consistency test-continuation test-transformation \
	rest-build-local rest-build-develop rest-build-baseline \
	rest-ensure-servers rest-ensure-baseline rest-serve rest-stop rest-info rest-run-tests \
	test-rest-local test-rest-dev-branch test-rest-fuzz test-rest-manual test-rest-loki \
	generate-loki rest-clean clean-venvs clean
