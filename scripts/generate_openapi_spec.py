#!/usr/bin/env python3
# ruff: noqa: T201
"""Generate OpenAPI spec offline without running the server.

Thin wrapper around graphsenselib.web.app.create_spec_app().
Used by the pre-commit hook to regenerate the Python client.
"""

import json

from graphsenselib.web.app import create_spec_app


def main():
    app = create_spec_app()
    schema = app.openapi()
    print(json.dumps(schema, indent=2))


if __name__ == "__main__":
    main()
