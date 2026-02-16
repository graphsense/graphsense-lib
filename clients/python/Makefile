-include .env

GENERATOR_VERSION ?= v7.19.0
TEMPLATES_DIR ?= templates
COMPAT_DIR ?= compat


# v7 generator with backward compatibility patches (offline - no server needed)
generate-openapi-client:
	@echo "Generating OpenAPI 3.1 spec offline..."
	cd ../.. && uv run python scripts/generate_openapi_spec.py > clients/python/openapi.json.tmp
	@echo "Converting to OpenAPI 3.0.3 for generator compatibility..."
	cd ../.. && uv run python scripts/convert_openapi_31_to_30.py < clients/python/openapi.json.tmp > clients/python/openapi.json.30.tmp
	@echo "Modifying server URL to production..."
	jq '.servers[0].url = "https://api.iknaio.com"' openapi.json.30.tmp > openapi.json.modified
	@API_VERSION=$$(jq -r '.info.version' openapi.json.30.tmp) && \
	echo "Generating client with $(GENERATOR_VERSION) (API version: $$API_VERSION)..." && \
	docker run --rm \
		-v "${PWD}:/build:Z" \
		-v "${PWD}/$(TEMPLATES_DIR):/templates:Z" \
		openapitools/openapi-generator-cli:$(GENERATOR_VERSION) \
		generate -i /build/openapi.json.modified \
		-g python \
		-t /templates \
		-o /build \
		--additional-properties=packageName=graphsense \
		--additional-properties=projectName=graphsense-python \
		--additional-properties=packageVersion=$$API_VERSION \
		--additional-properties=library=urllib3 && \
	echo "Fixing project name and version in pyproject.toml..." && \
	sed -i 's/^name = "graphsense"/name = "graphsense-python"/' pyproject.toml && \
	sed -i "s/^version = .*/version = \"$$API_VERSION\"/" pyproject.toml
	@echo "Applying backward compatibility patches..."
	uv run $(COMPAT_DIR)/patch_compat.py .
	@echo "Verifying backward compatibility..."
	uv run $(COMPAT_DIR)/test_compat.py .
	@echo "Cleaning up temporary files..."
	rm -f openapi.json.tmp openapi.json.30.tmp openapi.json.modified

# Just run the compatibility tests (useful during development)
test-compat:
	uv run $(COMPAT_DIR)/test_compat.py .

run-examples:
	API_KEY=$(API_KEY) uv run test_examples.py

.PHONY: generate-openapi-client test-compat run-examples
