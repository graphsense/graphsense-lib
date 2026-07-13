SHELL := /bin/bash

# ============================================================================
# Shared configuration
# ============================================================================

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
# Delta-update: UTXO transformed-keyspace cross-version regression
# ============================================================================

# Reference release of graphsense-lib used as the regression baseline. Defaults
# to v2.12.3 because that's the last release before the UTXO delta-update
# perf rewrite -- override per run when you want to compare against a
# different version.
DELTA_UPDATE_REF_VERSION ?= v2.12.3
DELTA_UPDATE_CURRENCIES ?= btc

test-delta-update: ## Delta-update: current vs reference release on UTXO transformed keyspace
	DELTA_UPDATE_REF_VERSION=$(DELTA_UPDATE_REF_VERSION) \
	DELTA_UPDATE_CURRENCIES=$(DELTA_UPDATE_CURRENCIES) \
	GSLIB_PATH=$(GSLIB_PATH) \
	uv run pytest tests/delta_update/ -v -s -m delta_update

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
	$(MAKE) test-ingest-deltalake || ret=1; \
	$(MAKE) test-ingest-sink-consistency || ret=1; \
	$(MAKE) test-ingest-split-continuation || ret=1; \
	$(MAKE) test-ingest-transformation || ret=1; \
	exit $$ret

# ============================================================================
# REST API regression tests
# ============================================================================

# Ports for locally served sides
REST_CURRENT_PORT ?= 19100
REST_BASELINE_PORT ?= 19101

# Config file for locally served sides
REST_CONFIG_FILE ?= $(PWD)/../../instance/config.yaml

# Main entry point: compare any two API versions/deployments.
#   make rest REF=<reference> CUR=<current> DEPTH=<quick|standard|full>
# REF/CUR: git tag/branch/commit, 'local' (working tree), api.iknaio.com,
# api.test.iknaio.com, or any http(s) URL. Run without args for full help.
rest: ## Compare two REST versions: make rest REF=.. CUR=.. DEPTH=quick|standard|full (no args = help)
ifeq ($(and $(REF),$(CUR),$(DEPTH)),)
	@./scripts/rest_suite.sh --help
else
	@REST_CONFIG_FILE="$(REST_CONFIG_FILE)" \
	REST_CURRENT_PORT="$(REST_CURRENT_PORT)" \
	REST_BASELINE_PORT="$(REST_BASELINE_PORT)" \
	./scripts/rest_suite.sh "$(REF)" "$(CUR)" "$(DEPTH)"
endif

rest-stop: ## Stop REST server containers
	-docker stop gs-rest-current gs-rest-baseline 2>/dev/null || true

generate-loki: ## Generate Loki test file from production logs (requires LOKI_URL)
	uv run scripts/generate_loki_tests.py

# ============================================================================
# Cleanup
# ============================================================================

clean-venvs: ## Remove cached test virtual environments
	rm -rf /tmp/gslib-deltalake-testvenvs

clean-rest: rest-stop ## Remove REST Docker images
	-docker images -q 'gslib-rest' | xargs -r docker rmi 2>/dev/null || true

clean: clean-rest clean-venvs ## Remove all test artifacts

# ============================================================================
# .PHONY
# ============================================================================

.PHONY: help install lint format \
	test-ingest test-ingest-cassandra test-ingest-deltalake \
	test-ingest-deltalake-cross-version test-ingest-deltalake-performance \
	test-ingest-sink-consistency test-ingest-split-continuation test-ingest-transformation \
	test-delta-update \
	rest rest-stop generate-loki \
	clean-venvs clean-rest clean
