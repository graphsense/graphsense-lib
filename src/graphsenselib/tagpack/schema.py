import datetime
import json
from json import JSONDecodeError

from graphsenselib.tagpack import ValidationError

# Caches
_type_def_cache: dict[int, dict[str, dict]] = {}  # udts_id -> {item_type -> definition}
_mandatory_fields_cache: dict[int, frozenset[str]] = {}  # schema_id -> mandatory fields


def _get_type_def_cache(udts: dict) -> dict[str, dict]:
    """Get or create type definition cache for a udts instance."""
    udts_id = id(udts)
    if udts_id not in _type_def_cache:
        _type_def_cache[udts_id] = {}
    return _type_def_cache[udts_id]


def load_field_type_definition(udts, item_type):
    cache = _get_type_def_cache(udts)
    if item_type in cache:
        return cache[item_type]

    if item_type.startswith("@"):
        fd = udts.get(item_type[1:])
        if fd is None:
            raise ValidationError(f"No type {item_type[1:]} found in the schema.")
        result = fd
    else:
        result = {"type": item_type}

    cache[item_type] = result
    return result


def _get_mandatory_fields(schema_def: dict) -> frozenset[str]:
    """Get mandatory fields for a schema definition (cached)."""
    schema_id = id(schema_def)
    if schema_id not in _mandatory_fields_cache:
        _mandatory_fields_cache[schema_id] = frozenset(
            k for k, v in schema_def.items() if bool(v.get("mandatory", False))
        )
    return _mandatory_fields_cache[schema_id]


def check_type_list_items(udts, field_name, field_definition, lst):
    if "item_type" in field_definition:
        item_def = load_field_type_definition(udts, field_definition["item_type"])
        for i, x in enumerate(lst):
            check_type(udts, f"{field_name}[{i}]", item_def, x)


def check_type_dict(udts, field_name, field_definition, dct):
    if "item_type" in field_definition:
        fd_def = load_field_type_definition(udts, field_definition["item_type"])
        if type(fd_def) is str:
            raise ValidationError(f"Type of dict {field_name} is a basic type {fd_def}")

        # Use cached mandatory fields
        mandatory_fields = _get_mandatory_fields(fd_def)
        for field in mandatory_fields:
            if field not in dct:
                raise ValidationError(f"Mandatory field {field} not in {dct}")

        for k, v in dct.items():
            fd = fd_def.get(k, None)
            if fd is not None:
                check_type(udts, k, fd, v)


def check_type(udts, field_name, field_definition, value):
    """Checks whether a field's type matches the definition"""
    schema_type = field_definition["type"]

    if schema_type == "text":
        if not isinstance(value, str):
            raise ValidationError("Field {} must be of type text".format(field_name))
        if len(value.strip()) == 0:
            raise ValidationError("Empty value in text field {}".format(field_name))

    elif schema_type == "datetime":
        if not isinstance(value, datetime.date) and not isinstance(
            value, datetime.datetime
        ):
            raise ValidationError(
                f"Field {field_name} must be of type datetime. Found {type(value)}"
            )

    elif schema_type == "boolean":
        if not isinstance(value, bool):
            raise ValidationError(f"Field {field_name} must be of type boolean")

    elif schema_type == "list":
        if not isinstance(value, list):
            raise ValidationError(f"Field {field_name} must be of type list")
        check_type_list_items(udts, field_name, field_definition, value)

    elif schema_type == "json_text":
        try:
            json_data = json.loads(value)
        except JSONDecodeError as e:
            raise ValidationError(
                f"Invalid JSON in field {field_name} with value {value}: {e}"
            )
        check_type_dict(udts, field_name, field_definition, json_data)

    elif schema_type == "dict":
        check_type_dict(udts, field_name, field_definition, value)

    else:
        raise ValidationError("Unsupported schema type {}".format(schema_type))

    return True
