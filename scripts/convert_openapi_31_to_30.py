#!/usr/bin/env python3
# ruff: noqa: T201
"""Convert an OpenAPI 3.1.0 spec to OpenAPI 3.0.3.

Reads JSON from stdin (or a file argument), writes converted JSON to stdout.
Generic converter — not GraphSense-specific.

Handled conversions (applied bottom-up per dict node):
  - openapi version: "3.1.0" → "3.0.3"
  - nullable simple type: anyOf[{type: X, ...}, {type: null}] → merged schema + nullable: true
  - nullable $ref: anyOf[{$ref: ...}, {type: null}] → allOf[{$ref}] + nullable: true
  - nullable union: anyOf[A, B, ..., {type: null}] (2+ non-null) → anyOf[A, B, ...] + nullable: true
  - const: val → enum: [val]
  - examples: [val, ...] → example: val
"""

import json
import sys


def _is_null_schema(schema):
    """Check if a schema represents the null type."""
    return isinstance(schema, dict) and schema.get("type") == "null"


def _walk_and_convert(obj):
    """Recursively walk the spec and apply 3.1→3.0 conversions bottom-up."""
    if isinstance(obj, list):
        return [_walk_and_convert(item) for item in obj]

    if not isinstance(obj, dict):
        return obj

    # Recurse into children first (bottom-up)
    obj = {k: _walk_and_convert(v) for k, v in obj.items()}

    # --- Version ---
    if obj.get("openapi") == "3.1.0":
        obj["openapi"] = "3.0.3"

    # --- const → enum ---
    if "const" in obj:
        obj["enum"] = [obj.pop("const")]

    # --- examples → example ---
    if "examples" in obj and isinstance(obj["examples"], list) and obj["examples"]:
        obj["example"] = obj["examples"][0]
        del obj["examples"]

    # --- anyOf with null → nullable ---
    if "anyOf" in obj and isinstance(obj["anyOf"], list):
        any_of = obj["anyOf"]
        null_schemas = [s for s in any_of if _is_null_schema(s)]
        non_null_schemas = [s for s in any_of if not _is_null_schema(s)]

        if null_schemas and non_null_schemas:
            del obj["anyOf"]

            if len(non_null_schemas) == 1:
                single = non_null_schemas[0]

                if "$ref" in single and len(single) == 1:
                    # Nullable $ref → allOf + nullable
                    obj["allOf"] = [single]
                else:
                    # Nullable simple type → merge properties + nullable
                    obj.update(single)
            else:
                # Nullable union (2+ non-null) → keep anyOf without null + nullable
                obj["anyOf"] = non_null_schemas

            obj["nullable"] = True

    return obj


def main():
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            spec = json.load(f)
    else:
        spec = json.load(sys.stdin)

    converted = _walk_and_convert(spec)
    print(json.dumps(converted, indent=2))


if __name__ == "__main__":
    main()
