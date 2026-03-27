SHELL := /bin/bash

# ============================================================================
# Shared configuration
# ============================================================================

GSLIB_REPO := https://github.com/graphsense/graphsense-lib.git
GSLIB_PATH := $(shell cd ../.. && pwd)

# Stable reference version for cross-version tests (cassandra, deltalake).
# The "current" version is always whatever is in GSLIB_PATH (incl. uncommitted).
REF_VERSION ?= v25.11.18

# Restrict all suites to specific currencies (comma-separated).
# Per-suite env vars (e.g., CASSANDRA_CURRENCIES) still take precedence.
CURRENCIES ?= btc,eth,ltc,bch,zec,trx

# ============================================================================
# Help (default target)
# ============================================================================

.DEFAULT_GOAL := help

help: ## Show available targets and options
	@echo "Usage: make <target> [VAR=value ...]"
	@echo ""
	@echo "Options:"
	@echo "  CURRENCIES=btc,eth        Restrict all suites to specific currencies (default: all)"
	@echo "  REF_VERSION=v25.11.18     Stable reference for cross-version tests"
	@echo "  DELTA_RANGE_CATEGORIES=.. Block range categories: genesis,old,mid,new,protocol (default: mid)"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-40s\033[0m %s\n", $$1, $$2}'

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
# Ingest: Cassandra cross-version
# ============================================================================

CASSANDRA_REF_VERSION ?= $(REF_VERSION)
CASSANDRA_CURRENCIES ?= $(CURRENCIES)

test-ingest-cassandra: ## Ingest to Cassandra: verify current matches reference version
	CASSANDRA_REF_VERSION=$(CASSANDRA_REF_VERSION) \
	CASSANDRA_CURRENCIES=$(CASSANDRA_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/cassandra/ -v -s -m cassandra

# ============================================================================
# Ingest: Delta Lake cross-version compatibility
# ============================================================================

DELTA_REF_VERSION ?= $(REF_VERSION)
DELTA_CURRENCIES ?= $(CURRENCIES)
DELTA_RANGE_CATEGORIES ?= mid

test-ingest-deltalake-cross-version: ## Ingest to Delta Lake: verify current matches reference version
	DELTA_REF_VERSION=$(DELTA_REF_VERSION) \
	DELTA_CURRENCIES=$(DELTA_CURRENCIES) \
	DELTA_RANGE_CATEGORIES=$(DELTA_RANGE_CATEGORIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/deltalake/ -v -s -m deltalake

# ============================================================================
# Ingest: Delta Lake performance
# ============================================================================

test-ingest-deltalake-performance: ## Ingest to Delta Lake: measure throughput (blocks/s)
	DELTA_REF_VERSION=$(DELTA_REF_VERSION) \
	DELTA_CURRENCIES=$(DELTA_CURRENCIES) \
	DELTA_RANGE_CATEGORIES=$(DELTA_RANGE_CATEGORIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/deltalake/test_performance.py -v -s -m deltalake_perf

# ============================================================================
# Ingest: Delta Lake (all — cross-version + performance)
# ============================================================================

test-ingest-deltalake: ## Ingest to Delta Lake: cross-version + performance
	@$(MAKE) test-ingest-deltalake-cross-version
	@$(MAKE) test-ingest-deltalake-performance

# ============================================================================
# Ingest: Sink consistency (dual-sink vs individual)
# ============================================================================

SINK_CONSISTENCY_CURRENCIES ?= $(CURRENCIES)

test-ingest-sink-consistency: ## Ingest sinks: verify dual-sink output matches individual sinks
	SINK_CONSISTENCY_CURRENCIES=$(SINK_CONSISTENCY_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/sink_consistency/ -v -s -m sink_consistency

# ============================================================================
# Ingest: Split-ingest continuation
# ============================================================================

CONTINUATION_CURRENCIES ?= $(CURRENCIES)

test-ingest-split-continuation: ## Ingest continuation: verify split-ingest matches one-shot
	CONTINUATION_CURRENCIES=$(CONTINUATION_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/continuation/ -v -s -m continuation

# ============================================================================
# Ingest: Transformation (Delta Lake → PySpark → Cassandra)
# ============================================================================

TRANSFORMATION_CURRENCIES ?= $(CURRENCIES)

test-ingest-transformation: ## Ingest transformation: Delta Lake → PySpark → Cassandra equivalence
	TRANSFORMATION_CURRENCIES=$(TRANSFORMATION_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/transformation/ -v -s -m transformation

# ============================================================================
# Ingest: All suites combined
# ============================================================================

test-ingest: ## Run all ingest regression tests
	@echo "========================================================================"
	@echo "  Running all ingest regression tests"
	@echo "  Currencies: $(CURRENCIES)"
	@echo "  Reference:  $(REF_VERSION)"
	@echo "========================================================================"
	@ret=0; \
	$(MAKE) test-ingest-cassandra || ret=1; \
	$(MAKE) test-ingest-deltalake-cross-version || ret=1; \
	$(MAKE) test-ingest-sink-consistency || ret=1; \
	$(MAKE) test-ingest-split-continuation || ret=1; \
	$(MAKE) test-ingest-transformation || ret=1; \
	exit $$ret

# ============================================================================
# REST API regression tests
# ============================================================================

# Baseline version: tag, branch, or commit hash (required for build targets)
REST_BASELINE ?=

# Docker image names
IMAGE_CURRENT := gslib-test:current
IMAGE_BASELINE := gslib-test:baseline

# Ports
REST_CURRENT_PORT ?= 19100
REST_BASELINE_PORT ?= 19101

# Config file for REST servers
REST_CONFIG_FILE ?= $(PWD)/../../instance/config.yaml

test-rest-fuzz: rest-ensure-servers ## REST API: fuzz regression (auto-generated endpoint comparison)
	CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
	uv run pytest tests/rest/test_baseline_regression.py -v -m regression

test-rest-manual: rest-ensure-servers ## REST API: manual regression (hand-written edge cases)
	CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
	uv run pytest tests/rest/test_manual_regression.py -v -m regression

test-rest-loki: rest-ensure-servers ## REST API: Loki-generated tests (from production logs)
	@if [ -f tests/rest/test_loki_generated.py ]; then \
		CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
		BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
		uv run pytest tests/rest/test_loki_generated.py -v -m loki_generated; \
	else \
		echo "Skipping: run 'make generate-loki' first to generate test file"; \
	fi

test-rest: rest-ensure-servers ## REST API: run all REST regression tests
	@ret=0; \
	$(MAKE) test-rest-fuzz || ret=1; \
	$(MAKE) test-rest-manual || ret=1; \
	$(MAKE) test-rest-loki || ret=1; \
	exit $$ret

generate-loki: ## Generate Loki test file from production logs (requires LOKI_URL)
	uv run scripts/generate_loki_tests.py

# --- REST infrastructure (build, serve, stop) ---

rest-build: ## Build both REST Docker images (GSLIB= REST_BASELINE=)
	@$(MAKE) rest-build-current
	@$(MAKE) rest-build-baseline

rest-build-current: ## Build current REST image from local repo
	@echo "Building current from: $(GSLIB_PATH)"
	docker build -t $(IMAGE_CURRENT) $(GSLIB_PATH)

rest-build-baseline: ## Build baseline REST image (REST_BASELINE= required)
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
			echo "Error: No baseline image found. Build one first:"; \
			echo "  make rest-build-baseline REST_BASELINE=v26.01.0"; \
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

rest-info: ## Show REST server status
	@echo "========================================================================"
	@echo "  REST API regression servers"
	@echo "========================================================================"
	@echo "  Current image:    $(IMAGE_CURRENT) ($$(docker inspect --format='{{.Id}}' $(IMAGE_CURRENT) 2>/dev/null | cut -c8-19 || echo 'not built'))"
	@echo "  Baseline image:   $(IMAGE_BASELINE) ($$(docker inspect --format='{{.Id}}' $(IMAGE_BASELINE) 2>/dev/null | cut -c8-19 || echo 'not built'))"
	@echo "  Current server:   http://localhost:$(REST_CURRENT_PORT) ($$(docker ps --format '{{.Status}}' -f name=gs-current 2>/dev/null || echo 'stopped'))"
	@echo "  Baseline server:  http://localhost:$(REST_BASELINE_PORT) ($$(docker ps --format '{{.Status}}' -f name=gs-baseline 2>/dev/null || echo 'stopped'))"
	@echo "  Config file:      $(REST_CONFIG_FILE)"
	@echo "========================================================================"

# ============================================================================
# Cleanup
# ============================================================================

clean-venvs: ## Remove cached test virtual environments
	rm -rf /tmp/gslib-deltalake-testvenvs

clean-rest: rest-stop ## Remove REST Docker images and temp clones
	-docker rmi $(IMAGE_CURRENT) $(IMAGE_BASELINE) 2>/dev/null || true
	-rm -rf /tmp/gslib-baseline

clean: clean-rest clean-venvs ## Remove all test artifacts

# ============================================================================
# .PHONY
# ============================================================================

.PHONY: help install lint format \
	test-ingest test-ingest-cassandra test-ingest-deltalake \
	test-ingest-deltalake-cross-version test-ingest-deltalake-performance \
	test-ingest-sink-consistency test-ingest-split-continuation test-ingest-transformation \
	test-rest test-rest-fuzz test-rest-manual test-rest-loki \
	generate-loki \
	rest-build rest-build-current rest-build-baseline \
	rest-ensure-servers rest-ensure-baseline rest-serve rest-stop rest-info \
	clean-venvs clean-rest clean
