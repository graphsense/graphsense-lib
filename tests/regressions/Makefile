SHELL := /bin/bash

# Required: baseline version (tag or commit hash) of graphsense-lib
BASELINE ?= 6c9f1ef2bc304e42bd2bb96f61b71a0fd9fe2959

# Branch to test against baseline
CURRENT_BRANCH ?= feature/web

# Optional: local graphsense-lib path (for build-local)
GSLIB ?=

# Ports (high ports to avoid conflicts)
CURRENT_PORT ?= 19100
BASELINE_PORT ?= 19101

# Docker image names
IMAGE_CURRENT := gslib-test:current
IMAGE_BASELINE := gslib-test:baseline

# Config (graphsense-REST format, mounted into container)
CONFIG_FILE ?= $(PWD)/config.yaml

# Repo
GSLIB_REPO := https://github.com/graphsense/graphsense-lib.git

.PHONY: build-local build-develop build-baseline serve stop test-local test-dev-branch run-tests test-rest-fuzz test-rest-manual test-rest-loki test-rest-all clean ensure-servers ensure-baseline info

# Build current from local path (requires GSLIB)
build-local:
	@if [ -z "$(GSLIB)" ]; then \
		echo "Error: GSLIB not set. Usage: make build-local GSLIB=/path/to/graphsense-lib"; \
		exit 1; \
	fi
	@echo "Building current from local: $(GSLIB)"
	docker build -t $(IMAGE_CURRENT) $(GSLIB)

# Build current from feature/web branch (REST API lives here)
build-develop:
	@echo "Building current from $(CURRENT_BRANCH) branch..."
	@rm -rf /tmp/gslib-current && \
	git clone --depth 1 --branch $(CURRENT_BRANCH) $(GSLIB_REPO) /tmp/gslib-current && \
	docker build -t $(IMAGE_CURRENT) /tmp/gslib-current

# Build baseline version (requires BASELINE=<tag|hash>)
build-baseline:
	@if [ -z "$(BASELINE)" ]; then \
		echo "Error: BASELINE not set. Usage: make test BASELINE=v26.01.0"; \
		exit 1; \
	fi
	@echo "Building baseline: $(BASELINE)"
	@rm -rf /tmp/gslib-baseline && \
	git clone $(GSLIB_REPO) /tmp/gslib-baseline && \
	cd /tmp/gslib-baseline && git checkout $(BASELINE) && \
	docker build -t $(IMAGE_BASELINE) /tmp/gslib-baseline

# Ensure baseline image exists
ensure-baseline:
	@if ! docker image inspect $(IMAGE_BASELINE) >/dev/null 2>&1; then \
		if [ -z "$(BASELINE)" ]; then \
			echo "Error: BASELINE not set and no baseline image exists."; \
			echo "Usage: make test-local BASELINE=v26.01.0 GSLIB=/path/to/repo"; \
			exit 1; \
		fi; \
		$(MAKE) build-baseline; \
	fi

# Ensure servers are running (start if not)
ensure-servers: ensure-baseline
	@if [ ! -f "$(CONFIG_FILE)" ] && [ ! -L "$(CONFIG_FILE)" ]; then \
		echo "Error: CONFIG_FILE not found at $(CONFIG_FILE)"; \
		exit 1; \
	fi
	@REAL_CONFIG=$$(readlink -f "$(CONFIG_FILE)") && \
	if ! docker ps --format '{{.Names}}' | grep -q '^gs-current$$'; then \
		echo "Starting current server on port $(CURRENT_PORT)..."; \
		docker run -d --name gs-current --rm --network=host \
			-v "$$REAL_CONFIG:/config.yaml:ro" \
			-e CONFIG_FILE=/config.yaml -e NUM_WORKERS=1 -e NUM_THREADS=1 \
			$(IMAGE_CURRENT) \
			sh -c 'gunicorn -c /opt/gunicorn-conf.py --bind 0.0.0.0:$(CURRENT_PORT) "graphsenselib.web.app:create_app(\"$$CONFIG_FILE\")" --worker-class uvicorn.workers.UvicornWorker'; \
	fi && \
	if ! docker ps --format '{{.Names}}' | grep -q '^gs-baseline$$'; then \
		echo "Starting baseline server on port $(BASELINE_PORT)..."; \
		docker run -d --name gs-baseline --rm --network=host \
			-v "$$REAL_CONFIG:/config.yaml:ro" \
			-e CONFIG_FILE=/config.yaml -e NUM_WORKERS=1 -e NUM_THREADS=1 \
			$(IMAGE_BASELINE) \
			sh -c 'gunicorn -c /opt/gunicorn-conf.py --bind 0.0.0.0:$(BASELINE_PORT) "graphsenselib.web.app:create_app(\"$$CONFIG_FILE\")" --worker-class uvicorn.workers.UvicornWorker'; \
	fi
	@echo "Waiting for servers to be ready..."
	@ready=0; \
	for i in $$(seq 1 60); do \
		if curl -s http://localhost:$(CURRENT_PORT)/stats >/dev/null 2>&1 && \
		   curl -s http://localhost:$(BASELINE_PORT)/stats >/dev/null 2>&1; then \
			ready=1; break; \
		fi; \
		sleep 2; \
	done; \
	if [ "$$ready" -ne 1 ]; then \
		echo "Error: servers did not become ready in time."; \
		echo "--- current logs ---"; docker logs gs-current 2>&1 | tail -20; \
		echo "--- baseline logs ---"; docker logs gs-baseline 2>&1 | tail -20; \
		$(MAKE) stop; \
		exit 1; \
	fi
	@echo "Current:  http://localhost:$(CURRENT_PORT)"
	@echo "Baseline: http://localhost:$(BASELINE_PORT)"

serve: ensure-servers

stop:
	-docker stop gs-current gs-baseline 2>/dev/null || true

# Tests - use test-local or test-dev-branch
test-local: build-local build-baseline ensure-servers
	@$(MAKE) run-tests; ret=$$?; $(MAKE) stop; exit $$ret

test-dev-branch: build-develop build-baseline ensure-servers
	@$(MAKE) run-tests; ret=$$?; $(MAKE) stop; exit $$ret

info:
	@echo "========================================================================"
	@echo "  iknaio-tests-nightly"
	@echo "========================================================================"
	@echo "  Date:             $$(date -Iseconds)"
	@echo "  Host:             $$(hostname)"
	@echo "  Current branch:   $(CURRENT_BRANCH)"
	@echo "  Baseline ref:     $(BASELINE)"
	@echo "  Current commit:   $$(docker inspect --format='{{.Config.Labels}}' $(IMAGE_CURRENT) 2>/dev/null | grep -oP 'commit:\K[a-f0-9]+' || (cd /tmp/gslib-current 2>/dev/null && git rev-parse --short HEAD || echo 'n/a'))"
	@echo "  Baseline commit:  $$(docker inspect --format='{{.Config.Labels}}' $(IMAGE_BASELINE) 2>/dev/null | grep -oP 'commit:\K[a-f0-9]+' || (cd /tmp/gslib-baseline 2>/dev/null && git rev-parse --short HEAD || echo 'n/a'))"
	@echo "  Current image:    $(IMAGE_CURRENT) ($$(docker inspect --format='{{.Id}}' $(IMAGE_CURRENT) 2>/dev/null | cut -c8-19 || echo 'n/a'))"
	@echo "  Baseline image:   $(IMAGE_BASELINE) ($$(docker inspect --format='{{.Id}}' $(IMAGE_BASELINE) 2>/dev/null | cut -c8-19 || echo 'n/a'))"
	@echo "  Current server:   http://localhost:$(CURRENT_PORT)"
	@echo "  Baseline server:  http://localhost:$(BASELINE_PORT)"
	@echo "  Config file:      $(CONFIG_FILE) -> $$(readlink -f $(CONFIG_FILE) 2>/dev/null || echo '(not a symlink)')"
	@echo "========================================================================"

run-tests: info
	@$(MAKE) test-rest-fuzz test-rest-manual test-rest-loki

test-rest-fuzz: ensure-servers
	CURRENT_SERVER=http://localhost:$(CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(BASELINE_PORT) \
	uv run pytest tests/rest/test_baseline_regression.py -v -m regression

test-rest-manual: ensure-servers
	CURRENT_SERVER=http://localhost:$(CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(BASELINE_PORT) \
	uv run pytest tests/rest/test_manual_regression.py -v -m regression

test-rest-loki: ensure-servers
	CURRENT_SERVER=http://localhost:$(CURRENT_PORT) \
	BASELINE_SERVER=http://localhost:$(BASELINE_PORT) \
	uv run pytest tests/rest/test_loki_generated.py -v -m loki_generated

test-rest-all: ensure-servers
	@$(MAKE) test-rest-fuzz test-rest-manual test-rest-loki; ret=$$?; $(MAKE) stop; exit $$ret

# Generate Loki tests from production logs (requires LOKI_URL)
generate-loki:
	uv run scripts/generate_loki_tests.py

clean: stop
	-docker rmi $(IMAGE_CURRENT) $(IMAGE_BASELINE) 2>/dev/null || true
	-rm -rf /tmp/gslib-current /tmp/gslib-baseline
