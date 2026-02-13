SHELL := /bin/bash

# Repo
GSLIB_REPO := https://github.com/graphsense/graphsense-lib.git

# ============================================================================
# REST API regression tests
# ============================================================================

# Required: baseline version (tag or commit hash) of graphsense-lib
REST_BASELINE ?= 6c9f1ef2bc304e42bd2bb96f61b71a0fd9fe2959

# Branch to test against baseline
REST_CURRENT_BRANCH ?= feature/web

# Optional: local graphsense-lib path (for rest-build-local)
GSLIB ?=

# Ports (high ports to avoid conflicts)
REST_CURRENT_PORT ?= 19100
REST_BASELINE_PORT ?= 19101

# Docker image names
IMAGE_CURRENT := gslib-test:current
IMAGE_BASELINE := gslib-test:baseline

# Config (graphsense-REST format, mounted into container)
REST_CONFIG_FILE ?= $(PWD)/config.yaml

.PHONY: rest-build-local rest-build-develop rest-build-baseline rest-serve rest-stop \
	test-rest-local test-rest-dev-branch rest-run-tests \
	test-rest-fuzz test-rest-manual test-rest-loki test-rest-all \
	rest-ensure-servers rest-ensure-baseline rest-info rest-clean \
	generate-loki test-deltalake clean-deltalake clean

# Build current from local path (requires GSLIB)
rest-build-local:
	@if [ -z "$(GSLIB)" ]; then \
		echo "Error: GSLIB not set. Usage: make rest-build-local GSLIB=/path/to/graphsense-lib"; \
		exit 1; \
	fi
	@echo "Building current from local: $(GSLIB)"
	docker build -t $(IMAGE_CURRENT) $(GSLIB)

# Build current from feature/web branch (REST API lives here)
rest-build-develop:
	@echo "Building current from $(REST_CURRENT_BRANCH) branch..."
	@rm -rf /tmp/gslib-current && \
	git clone --depth 1 --branch $(REST_CURRENT_BRANCH) $(GSLIB_REPO) /tmp/gslib-current && \
	docker build -t $(IMAGE_CURRENT) /tmp/gslib-current

# Build baseline version (requires REST_BASELINE=<tag|hash>)
rest-build-baseline:
	@if [ -z "$(REST_BASELINE)" ]; then \
		echo "Error: REST_BASELINE not set. Usage: make test REST_BASELINE=v26.01.0"; \
		exit 1; \
	fi
	@echo "Building baseline: $(REST_BASELINE)"
	@rm -rf /tmp/gslib-baseline && \
	git clone $(GSLIB_REPO) /tmp/gslib-baseline && \
	cd /tmp/gslib-baseline && git checkout $(REST_BASELINE) && \
	docker build -t $(IMAGE_BASELINE) /tmp/gslib-baseline

# Ensure baseline image exists
rest-ensure-baseline:
	@if ! docker image inspect $(IMAGE_BASELINE) >/dev/null 2>&1; then \
		if [ -z "$(REST_BASELINE)" ]; then \
			echo "Error: REST_BASELINE not set and no baseline image exists."; \
			echo "Usage: make test-rest-local REST_BASELINE=v26.01.0 GSLIB=/path/to/repo"; \
			exit 1; \
		fi; \
		$(MAKE) rest-build-baseline; \
	fi

# Ensure servers are running (start if not)
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

rest-serve: rest-ensure-servers

rest-stop:
	-docker stop gs-current gs-baseline 2>/dev/null || true

# REST tests — use test-rest-local or test-rest-dev-branch
test-rest-local: rest-build-local rest-build-baseline rest-ensure-servers
	@$(MAKE) rest-run-tests; ret=$$?; $(MAKE) rest-stop; exit $$ret

test-rest-dev-branch: rest-build-develop rest-build-baseline rest-ensure-servers
	@$(MAKE) rest-run-tests; ret=$$?; $(MAKE) rest-stop; exit $$ret

rest-info:
	@echo "========================================================================"
	@echo "  iknaio-tests-nightly — REST API"
	@echo "========================================================================"
	@echo "  Date:             $$(date -Iseconds)"
	@echo "  Host:             $$(hostname)"
	@echo "  Current branch:   $(REST_CURRENT_BRANCH)"
	@echo "  Baseline ref:     $(REST_BASELINE)"
	@echo "  Current commit:   $$(docker inspect --format='{{.Config.Labels}}' $(IMAGE_CURRENT) 2>/dev/null | grep -oP 'commit:\K[a-f0-9]+' || (cd /tmp/gslib-current 2>/dev/null && git rev-parse --short HEAD || echo 'n/a'))"
	@echo "  Baseline commit:  $$(docker inspect --format='{{.Config.Labels}}' $(IMAGE_BASELINE) 2>/dev/null | grep -oP 'commit:\K[a-f0-9]+' || (cd /tmp/gslib-baseline 2>/dev/null && git rev-parse --short HEAD || echo 'n/a'))"
	@echo "  Current image:    $(IMAGE_CURRENT) ($$(docker inspect --format='{{.Id}}' $(IMAGE_CURRENT) 2>/dev/null | cut -c8-19 || echo 'n/a'))"
	@echo "  Baseline image:   $(IMAGE_BASELINE) ($$(docker inspect --format='{{.Id}}' $(IMAGE_BASELINE) 2>/dev/null | cut -c8-19 || echo 'n/a'))"
	@echo "  Current server:   http://localhost:$(REST_CURRENT_PORT)"
	@echo "  Baseline server:  http://localhost:$(REST_BASELINE_PORT)"
	@echo "  Config file:      $(REST_CONFIG_FILE) -> $$(readlink -f $(REST_CONFIG_FILE) 2>/dev/null || echo '(not a symlink)')"
	@echo "========================================================================"

rest-run-tests: rest-info
	@$(MAKE) test-rest-fuzz test-rest-manual test-rest-loki

test-rest-fuzz: rest-ensure-servers
	CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
	uv run pytest tests/rest/test_baseline_regression.py -v -m regression

test-rest-manual: rest-ensure-servers
	CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
	uv run pytest tests/rest/test_manual_regression.py -v -m regression

test-rest-loki: rest-ensure-servers
	CURRENT_SERVER=http://localhost:$(REST_CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(REST_BASELINE_PORT) \
	uv run pytest tests/rest/test_loki_generated.py -v -m loki_generated

test-rest-all: rest-ensure-servers
	@$(MAKE) test-rest-fuzz test-rest-manual test-rest-loki; ret=$$?; $(MAKE) rest-stop; exit $$ret

# Generate Loki tests from production logs (requires LOKI_URL)
generate-loki:
	uv run scripts/generate_loki_tests.py

rest-clean: rest-stop
	-docker rmi $(IMAGE_CURRENT) $(IMAGE_BASELINE) 2>/dev/null || true
	-rm -rf /tmp/gslib-current /tmp/gslib-baseline

# ============================================================================
# Delta Lake cross-version compatibility tests
# ============================================================================

DELTA_REF_VERSION ?= v25.11.18
DELTA_CURRENCIES ?= btc,eth,ltc,bch,zec,trx

test-deltalake:
	DELTA_REF_VERSION=$(DELTA_REF_VERSION) \
	DELTA_CURRENCIES=$(DELTA_CURRENCIES) \
	GSLIB_PATH=$$(cd ../.. && pwd) \
	uv run pytest tests/deltalake/ -v -s -m deltalake

clean-deltalake:
	rm -rf /tmp/gslib-deltalake-testvenvs

# ============================================================================
# Top-level
# ============================================================================

clean: rest-clean clean-deltalake
