import datetime
import json
from json import JSONDecodeError

from tagpack import ValidationError


def load_field_type_definition(udts, item_type):
    if item_type.startswith("@"):
        fd = udts.get(item_type[1:])
        if fd is None:
            raise ValidationError(f"No type {item_type[1:]} found in the schema.")
        return fd
    else:
        return {"type": item_type}


def check_type_list_items(udts, field_name, field_definition, lst):
    if "item_type" in field_definition:
        for i, x in enumerate(lst):
            check_type(
                udts,
                f"{field_name}[{i}]",
                load_field_type_definition(udts, field_definition["item_type"]),
                x,
            )


def check_type_dict(udts, field_name, field_definition, dct):
    if "item_type" in field_definition:
        fd_def = load_field_type_definition(udts, field_definition["item_type"])

        if type(fd_def) is str:
            raise ValidationError(f"Type of dict {field_name} is a basic type {fd_def}")

        # check mandatory entries
        mandatory_fields = [
            k for k, v in fd_def.items() if bool(v.get("mandatory", False))
        ]

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
        if not isinstance(value, datetime.date):
            raise ValidationError(f"Field {field_name} must be of type datetime")
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
