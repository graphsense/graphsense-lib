#!/usr/bin/env python3
# ruff: noqa: T201
"""Generate OpenAPI spec offline without running the server.

This script creates a minimal FastAPI app instance and exports its OpenAPI schema.
Used by the pre-commit hook to regenerate the Python client.
"""

import json

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from graphsenselib.web.app import _convert_schema_names_to_snake_case
from graphsenselib.web.routes import (
    addresses,
    blocks,
    bulk,
    entities,
    general,
    rates,
    tags,
    tokens,
    txs,
)
from graphsenselib.web.version import __api_version__


def _convert_examples_to_example(obj):
    """Convert OpenAPI 3.1 'examples' arrays to OpenAPI 3.0 'example' values.

    OpenAPI Generator doesn't fully support 3.1's examples in schemas,
    so we convert examples: [value] to example: value for compatibility.
    """
    if isinstance(obj, dict):
        # If this dict has 'examples' array, convert to 'example'
        if "examples" in obj and isinstance(obj["examples"], list) and obj["examples"]:
            obj["example"] = obj["examples"][0]
            del obj["examples"]
        # Recurse into all values
        for value in obj.values():
            _convert_examples_to_example(value)
    elif isinstance(obj, list):
        for item in obj:
            _convert_examples_to_example(item)
    return obj


def create_minimal_app() -> FastAPI:
    """Create a minimal FastAPI app just for OpenAPI schema generation.

    This doesn't require database connections or config files.
    """
    app = FastAPI(
        title="GraphSense API",
        description="GraphSense API provides programmatic access to various cryptocurrency analytics features.",
        version=__api_version__,
    )

    # Register all routers (same as in the real app)
    app.include_router(general.router, tags=["general"])
    app.include_router(tags.router, tags=["tags"])
    app.include_router(addresses.router, prefix="/{currency}", tags=["addresses"])
    app.include_router(blocks.router, prefix="/{currency}", tags=["blocks"])
    app.include_router(entities.router, prefix="/{currency}", tags=["entities"])
    app.include_router(txs.router, prefix="/{currency}", tags=["txs"])
    app.include_router(rates.router, prefix="/{currency}", tags=["rates"])
    app.include_router(tokens.router, prefix="/{currency}", tags=["tokens"])
    app.include_router(bulk.router, prefix="/{currency}", tags=["bulk"])

    return app


def main():
    app = create_minimal_app()

    # Generate OpenAPI schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    # Add servers for client compatibility
    schema["servers"] = [{"url": ""}]

    # Add contact info
    schema["info"]["contact"] = {
        "email": "contact@ikna.io",
        "name": "Iknaio Cryptoasset Analytics GmbH",
    }
    schema["info"]["description"] = (
        "GraphSense API provides programmatic access to various ledgers' "
        "addresses, entities, blocks, transactions and tags for automated "
        "and highly efficient forensics tasks."
    )

    # Apply snake_case conversion for backward compatibility
    schema = _convert_schema_names_to_snake_case(schema)

    # Convert examples arrays to example for OpenAPI Generator compatibility
    schema = _convert_examples_to_example(schema)

    print(json.dumps(schema, indent=2))


if __name__ == "__main__":
    main()
