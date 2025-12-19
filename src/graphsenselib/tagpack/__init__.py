"""Module functions and classes for tagpack-tool"""

import sys

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader as SafeLoader

# Fast YAML loading using rapidyaml
import warnings

with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message="builtin type Swig.*has no __module__ attribute",
        category=DeprecationWarning,
    )
    import ryml as _ryml

if sys.version_info[:2] >= (3, 8):
    # TODO: Import directly (no need for conditional) when `python_requires = >= 3.8`
    from importlib.metadata import PackageNotFoundError, version  # pragma: no cover
else:
    from importlib_metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Use the graphsense-lib version since tagpack is now part of it
    dist_name = "graphsense-lib"
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError


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


def _check_duplicate_keys_ryml(tree, root_id):
    """Check for duplicate keys at the top level of ryml tree."""
    if not tree.is_map(root_id):
        return
    keys = set()
    for i in range(tree.num_children(root_id)):
        child_id = tree.child(root_id, i)
        key = tree.key(child_id)
        if key in keys:
            raise ValidationError(
                f"Duplicate {key.tobytes().decode()!r} key found in YAML."
            )
        keys.add(key)


def load_yaml_fast(file_path):
    """Load YAML using rapidyaml (~10x faster) for large files, else PyYAML."""
    import json
    import os
    import yaml

    file_size = os.path.getsize(file_path)

    # Use UniqueKeyLoader for small files (duplicate key detection)
    if file_size < 100 * 1024:
        with open(file_path, "r") as f:
            return yaml.load(f, UniqueKeyLoader)

    # Fast path: ryml -> check duplicates -> JSON -> json.loads
    with open(file_path, "rb") as f:
        content = f.read()
    tree = _ryml.parse_in_arena(content)
    _check_duplicate_keys_ryml(tree, tree.root_id())
    json_bytes = _ryml.emit_json_malloc(tree, tree.root_id())
    return json.loads(json_bytes)
