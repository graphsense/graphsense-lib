"""Module functions and classes for tagpack-tool"""

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader as SafeLoader

import logging
import re
import warnings

from importlib.metadata import PackageNotFoundError, version  # pragma: no cover

logger = logging.getLogger(__name__)

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


# YAML 1.1 reads an unquoted 0x... as a hexadecimal number. In tagpacks that is
# always an address or a hash, never a number, so it stays text: checked
# before PyYAML's own rules for scalars starting with "0" (the fast loader
# uses the same table).
_HEX_AS_TEXT = re.compile(r"^0x[0-9a-fA-F_]+$")
UniqueKeyLoader.yaml_implicit_resolvers = {
    first: list(rules) for first, rules in SafeLoader.yaml_implicit_resolvers.items()
}
UniqueKeyLoader.yaml_implicit_resolvers["0"].insert(
    0, ("tag:yaml.org,2002:str", _HEX_AS_TEXT)
)


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


class _NeedsPyYAML(Exception):
    """The file uses a feature the fast loader leaves to PyYAML."""


# Plain scalars recur a lot in packs (currencies, labels, booleans); their
# PyYAML value depends only on the text, so it is cached. Only immutable
# results (str, int, float, bool, None, dates) are stored.
_PLAIN_CACHE: dict = {}
_PLAIN_CACHE_MAX = 10_000
_STR_TAG = "tag:yaml.org,2002:str"


_PYYAML_RULES: tuple = ()


def _resolve_plain(text: str):
    """An unquoted scalar, read exactly as PyYAML's SafeLoader reads it."""
    global _PYYAML_RULES
    try:
        return _PLAIN_CACHE[text]
    except KeyError:
        pass
    import yaml

    if not _PYYAML_RULES:
        # UniqueKeyLoader's resolver rules and SafeLoader's constructor
        resolver = yaml.resolver.Resolver()
        resolver.yaml_implicit_resolvers = UniqueKeyLoader.yaml_implicit_resolvers
        _PYYAML_RULES = (resolver, yaml.constructor.SafeConstructor())
    resolver, constructor = _PYYAML_RULES
    tag = resolver.resolve(yaml.ScalarNode, text, (True, False))
    if tag == _STR_TAG:
        value = text
    else:
        # the scalar constructor directly: construct_object would remember
        # every node it ever built
        make = constructor.yaml_constructors.get(tag)
        if make is None:
            raise _NeedsPyYAML  # e.g. '=' or '<<' as a value: PyYAML reports it
        value = make(constructor, yaml.ScalarNode(tag, text))
    if len(_PLAIN_CACHE) < _PLAIN_CACHE_MAX:
        _PLAIN_CACHE[text] = value
    return value


def _ryml_scalar(tree, node, key: bool):
    if key:
        if tree.has_key_tag(node):
            raise _NeedsPyYAML
        text, plain = tree.key(node), tree.is_key_plain(node)
    else:
        if tree.has_val_tag(node):
            raise _NeedsPyYAML
        text, plain = tree.val(node), tree.is_val_plain(node)
    s = bytes(text).decode("utf-8") if text is not None else ""
    if plain or text is None:
        # unquoted, or empty (`key:` without a value, which PyYAML reads as None)
        return _resolve_plain(s)
    return s  # quoted or block scalar: always text, as in PyYAML


def _ryml_to_python(tree, node):
    """Python objects from a rapidyaml tree with PyYAML's YAML 1.1 rules."""
    import ryml

    if tree.is_map(node):
        out = {}
        child = tree.first_child(node)
        while child != ryml.NONE:
            k = _ryml_scalar(tree, child, key=True)
            if k in out:
                raise ValidationError(f"Duplicate {k!r} key found in YAML.")
            out[k] = _ryml_to_python(tree, child)
            child = tree.next_sibling(child)
        return out
    if tree.is_seq(node):
        out = []
        child = tree.first_child(node)
        while child != ryml.NONE:
            out.append(_ryml_to_python(tree, child))
            child = tree.next_sibling(child)
        return out
    if not tree.has_val(node):
        return None  # empty document
    return _ryml_scalar(tree, node, key=False)


def load_yaml_fast(file_path):
    """Load YAML with rapidyaml if available; same result as PyYAML.

    rapidyaml only parses. Unquoted values are resolved with PyYAML's own
    YAML 1.1 rules (yes/no, 0x1A, ~, .inf, dates, ...), quoted and block
    values stay text, aliases are expanded, and duplicate keys are rejected,
    so the result equals ``yaml.load(f, UniqueKeyLoader)``. Files with YAML
    tags (e.g. ``!include``), merge keys (``<<``) or several documents, and
    files rapidyaml cannot parse, are loaded with PyYAML.
    """
    import yaml

    with open(file_path, "rb") as f:
        content = f.read()
    if not RYML_AVAILABLE:
        return yaml.load(content.decode("utf-8"), UniqueKeyLoader)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        import ryml

    try:
        tree = ryml.parse_in_arena(content)
    except ryml.ExceptionParse:
        # rapidyaml is only the fast path, and its exception carries no
        # message (it prints line/column to stderr itself). PyYAML loads what
        # only rapidyaml rejects, and fails with line and column otherwise.
        logger.warning(f"{file_path}: fast YAML parser failed, retrying with PyYAML")
        return yaml.load(content.decode("utf-8"), UniqueKeyLoader)
    root = tree.root_id()
    try:
        if tree.is_stream(root) or b"<<" in content:
            # several documents, or a possible merge key (rapidyaml would merge
            # it while resolving aliases; PyYAML decides)
            raise _NeedsPyYAML
        tree.resolve()  # expand aliases
        return _ryml_to_python(tree, root)
    except _NeedsPyYAML:
        return yaml.load(content.decode("utf-8"), UniqueKeyLoader)
