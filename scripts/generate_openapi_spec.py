#!/usr/bin/env python3
# ruff: noqa: T201
"""Generate OpenAPI spec offline without running the server.

This script creates a minimal FastAPI app instance and exports its OpenAPI schema.
Used by the pre-commit hook to regenerate the Python client.
"""

import json

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from graphsenselib import __version__
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


def create_minimal_app() -> FastAPI:
    """Create a minimal FastAPI app just for OpenAPI schema generation.

    This doesn't require database connections or config files.
    """
    app = FastAPI(
        title="GraphSense API",
        description="GraphSense API provides programmatic access to various cryptocurrency analytics features.",
        version=__version__,
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

    print(json.dumps(schema, indent=2))


if __name__ == "__main__":
    main()
