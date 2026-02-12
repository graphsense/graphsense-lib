#!/usr/bin/env python3
# ruff: noqa: T201
"""Convert an OpenAPI 3.1.0 spec to OpenAPI 3.0.3.

Reads JSON from stdin (or a file argument), writes converted JSON to stdout.
Thin wrapper around graphsenselib.web.openapi_compat.convert_openapi_31_to_30().
"""

import json
import sys

from graphsenselib.web.openapi_compat import convert_openapi_31_to_30


def main():
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            spec = json.load(f)
    else:
        spec = json.load(sys.stdin)

    converted = convert_openapi_31_to_30(spec)
    print(json.dumps(converted, indent=2))


if __name__ == "__main__":
    main()
