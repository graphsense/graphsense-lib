"""Module functions and classes for tagpack-tool"""

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader as SafeLoader

import warnings

import re

from importlib.metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Use the graphsense-lib version since tagpack is now part of it
    dist_name = "graphsense-lib"
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError

_YAML_DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def get_version():
    return __version__


class TagPackFileError(Exception):
    """Class for TagPack file (structure) errors"""

    def __init__(self, message):
        super().__init__(message)


class ValidationError(Exception):
    """Class for schema validation errors"""

    def __init__(self, message):
        prefix = "Schema Validation Error: "
        if not message.startswith(prefix):
            message = f"{prefix}{message}"
        super().__init__(message)


class StorageError(Exception):
    """Class for Cassandra-related errors"""

    def __init__(self, message, nested_exception=None):
        super().__init__("Cassandra Error: " + message)
        self.nested_exception = nested_exception

    def __str__(self):
        msg = super(StorageError, self).__str__()
        if self.nested_exception:
            msg = msg + "\nError Details: " + str(self.nested_exception)
        return msg


# https://gist.github.com/pypt/94d747fe5180851196eb
class UniqueKeyLoader(SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ValidationError(f"Duplicate {key!r} key found in YAML.")
            mapping.add(key)
        return super().construct_mapping(node, deep)


def _dict_raise_on_duplicates(pairs):
    """Raise ValidationError on duplicate keys during JSON parsing."""
    d = {}
    for k, v in pairs:
        if k in d:
            raise ValidationError(f"Duplicate {k!r} key found in YAML.")
        d[k] = v
    return d


def _convert_yaml_dates(obj):
    """Recursively convert YYYY-MM-DD strings to datetime.date objects.

    This matches PyYAML SafeLoader behavior for dates while keeping
    rapidyaml behavior for other types (yes/no remain as strings).
    """
    from datetime import date

    if isinstance(obj, dict):
        return {k: _convert_yaml_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_yaml_dates(item) for item in obj]
    elif isinstance(obj, str):
        if _YAML_DATE_REGEX.match(obj):
            try:
                parts = obj.split("-")
                return date(int(parts[0]), int(parts[1]), int(parts[2]))
            except (ValueError, IndexError):
                pass
        return obj
    else:
        return obj


def _ryml_available():
    """Check if rapidyaml is available."""
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            import ryml  # noqa: F401

        return True
    except ImportError:
        return False


RYML_AVAILABLE = _ryml_available()


def load_yaml_fast(file_path):
    """Load YAML using rapidyaml if available, otherwise fall back to PyYAML.

    Note: When using rapidyaml, this produces slightly different types than PyYAML:
    - 'yes'/'no'/'on'/'off' remain as strings (PyYAML converts to bool)
    - 'YYYY-MM-DD' dates are converted to datetime.date (same as PyYAML)
    - 'true'/'false' are converted to bool (same as PyYAML)
    """
    import json

    if not RYML_AVAILABLE:
        import yaml

        with open(file_path, "r") as f:
            return yaml.load(f, UniqueKeyLoader)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        import ryml

    with open(file_path, "rb") as f:
        content = f.read()
    tree = ryml.parse_in_arena(content)
    json_bytes = ryml.emit_json_malloc(tree, tree.root_id())
    data = json.loads(json_bytes, object_pairs_hook=_dict_raise_on_duplicates)
    return _convert_yaml_dates(data)
