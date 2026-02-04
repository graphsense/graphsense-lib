#!/usr/bin/env python3
"""
Post-processing script for OpenAPI v7 generated Python client.

This script adds backward compatibility for the .value pattern used by v5 clients.
It creates CompatInt and CompatList classes that allow existing code like:
    tx.height.value
    tx.inputs.value
to continue working with the v7 generated client.
"""

import os
import re
import sys
from pathlib import Path


COMPAT_MODULE_CONTENT = '''\
"""
Backward compatibility types for v5 client migration.

These types provide .value property access for types that were previously
wrapped in ModelSimple classes (Height, Actors, TxInputs, etc.).
"""

from typing import TypeVar, Generic, Iterator, List, Any


class CompatInt(int):
    """
    Integer subclass that provides backward-compatible .value property.

    Used for fields like `height` that were previously wrapped in a Height model
    with a .value property.

    Examples:
        >>> h = CompatInt(12345)
        >>> h.value
        12345
        >>> h + 1
        12346
        >>> isinstance(h, int)
        True
    """

    @property
    def value(self) -> int:
        """Return self for backward compatibility with ModelSimple pattern."""
        return int(self)

    def to_str(self) -> str:
        """Return string representation for backward compatibility."""
        return str(self)

    # Arithmetic operations that preserve CompatInt type
    def __add__(self, other): return CompatInt(int(self) + other)
    def __radd__(self, other): return CompatInt(other + int(self))
    def __sub__(self, other): return CompatInt(int(self) - other)
    def __rsub__(self, other): return CompatInt(other - int(self))
    def __mul__(self, other): return CompatInt(int(self) * other)
    def __rmul__(self, other): return CompatInt(other * int(self))
    def __floordiv__(self, other): return CompatInt(int(self) // other)
    def __mod__(self, other): return CompatInt(int(self) % other)
    def __neg__(self): return CompatInt(-int(self))
    def __pos__(self): return CompatInt(+int(self))
    def __abs__(self): return CompatInt(abs(int(self)))

    # Serialization support (pickle/copy)
    def __reduce__(self):
        return (CompatInt, (int(self),))

    def __copy__(self):
        return CompatInt(int(self))

    def __deepcopy__(self, memo):
        return CompatInt(int(self))


T = TypeVar('T')


class CompatList(list, Generic[T]):
    """
    List subclass that provides backward-compatible .value property.

    Used for fields like `inputs`, `outputs`, `actors` that were previously
    wrapped in ModelSimple classes with a .value property.

    Examples:
        >>> items = CompatList([1, 2, 3])
        >>> items.value
        [1, 2, 3]
        >>> items.value[0]
        1
        >>> for item in items.value:
        ...     print(item)
        1
        2
        3
        >>> len(items.value)
        3
    """

    @property
    def value(self) -> List[T]:
        """Return self for backward compatibility with ModelSimple pattern."""
        return list(self)

    # Serialization support (pickle/copy)
    def __reduce__(self):
        return (CompatList, (list(self),))

    def __copy__(self):
        return CompatList(list(self))

    def __deepcopy__(self, memo):
        import copy
        return CompatList(copy.deepcopy(list(self), memo))


# Type aliases for backward compatibility with v5 client
# These were previously ModelSimple wrapper types
Height = CompatInt


class DictModel:
    """
    A wrapper that allows attribute access on dicts for backward compatibility.

    When oneOf/anyOf models store data as raw dicts, this wrapper allows
    accessing nested fields as attributes (like Pydantic models) rather than
    only via dict keys.

    Examples:
        >>> data = DictModel({'best_address_tag': {'label': 'Exchange'}})
        >>> data.best_address_tag.label
        'Exchange'
        >>> data['best_address_tag']['label']
        'Exchange'
    """

    def __init__(self, data: dict):
        object.__setattr__(self, '_data', data)
        object.__setattr__(self, '_cache', {})

    def __getattr__(self, name: str) -> Any:
        if name.startswith('_'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        data = object.__getattribute__(self, '_data')
        cache = object.__getattribute__(self, '_cache')
        # Return cached wrapped value if available
        if name in cache:
            return cache[name]
        if name in data:
            value = data[name]
            # Recursively wrap nested dicts and cache them
            if isinstance(value, dict):
                wrapped = DictModel(value)
                cache[name] = wrapped
                return wrapped
            elif isinstance(value, list):
                wrapped = [DictModel(item) if isinstance(item, dict) else item for item in value]
                cache[name] = wrapped
                return wrapped
            return value
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __getitem__(self, key: str) -> Any:
        data = object.__getattribute__(self, '_data')
        value = data[key]
        if isinstance(value, dict):
            return DictModel(value)
        elif isinstance(value, list):
            return [DictModel(item) if isinstance(item, dict) else item for item in value]
        return value

    def get(self, key: str = None, default: Any = None) -> Any:
        """Dict-style get() method."""
        if key is None:
            return self
        data = object.__getattribute__(self, '_data')
        if key in data:
            value = data[key]
            if isinstance(value, dict):
                return DictModel(value)
            elif isinstance(value, list):
                return [DictModel(item) if isinstance(item, dict) else item for item in value]
            return value
        return default

    def __repr__(self) -> str:
        data = object.__getattribute__(self, '_data')
        return f"DictModel({data!r})"

    def __bool__(self) -> bool:
        data = object.__getattribute__(self, '_data')
        return bool(data)

    def to_dict(self) -> dict:
        """Return the underlying dict."""
        return object.__getattribute__(self, '_data')

    # Dict protocol methods
    def __contains__(self, key: str) -> bool:
        return key in object.__getattribute__(self, '_data')

    def __iter__(self):
        return iter(object.__getattribute__(self, '_data'))

    def __len__(self) -> int:
        return len(object.__getattribute__(self, '_data'))

    def keys(self):
        return object.__getattribute__(self, '_data').keys()

    def values(self):
        return object.__getattribute__(self, '_data').values()

    def items(self):
        return object.__getattribute__(self, '_data').items()

    # Copy support
    def __copy__(self):
        return DictModel(object.__getattribute__(self, '_data').copy())

    def __deepcopy__(self, memo):
        import copy
        return DictModel(copy.deepcopy(object.__getattribute__(self, '_data'), memo))


# Monkey-patch Pydantic BaseModel to support dict-style access for backward compatibility
def _patch_pydantic_basemodel():
    """Add __getitem__ and get() to Pydantic BaseModel for backward compatibility with v5 client."""
    from pydantic import BaseModel

    if hasattr(BaseModel, '_compat_patched'):
        return

    def __getitem__(self, key):
        """Allow dict-style access to model fields for backward compatibility."""
        return getattr(self, key)

    def _compat_get(self, key=None, default=None):
        """Allow dict.get() style access for backward compatibility.

        If called with no key (e.g., model.get()), returns self to support
        the v5 async_req pattern where the async result's .get() returns the result.
        """
        if key is None:
            return self
        return getattr(self, key, default)

    BaseModel.__getitem__ = __getitem__
    BaseModel.get = _compat_get
    BaseModel._compat_patched = True


# Apply the patch when this module is imported
_patch_pydantic_basemodel()
'''


def create_compat_module(package_dir: Path) -> None:
    """Create the compat.py module in the package directory."""
    compat_path = package_dir / "compat.py"
    print(f"Creating {compat_path}")
    compat_path.write_text(COMPAT_MODULE_CONTENT)


def patch_model_init(models_dir: Path) -> None:
    """Patch the __init__.py in models to export compat types."""
    init_path = models_dir / "__init__.py"
    if not init_path.exists():
        print(f"Warning: {init_path} does not exist")
        return

    content = init_path.read_text()

    # Add import for compat types if not already present
    if "from graphsense.compat import" not in content:
        # Find a good place to insert - after other imports
        import_line = "from graphsense.compat import CompatInt, CompatList\n"

        # Insert after the last 'from graphsense.' import
        lines = content.split("\n")
        insert_idx = 0
        for i, line in enumerate(lines):
            if line.startswith("from graphsense."):
                insert_idx = i + 1

        if insert_idx > 0:
            lines.insert(insert_idx, import_line.rstrip())
            content = "\n".join(lines)
        else:
            # Fallback: add at the top after any initial comments/docstrings
            content = import_line + content

        print(f"Patching {init_path} to export compat types")
        init_path.write_text(content)


def patch_package_init(package_dir: Path) -> None:
    """Patch the main package __init__.py to import compat module early."""
    init_path = package_dir / "__init__.py"
    if not init_path.exists():
        return

    content = init_path.read_text()

    # Add compat import at the beginning of imports
    if "import graphsense.compat" not in content:
        print("  Adding compat import to package __init__.py")
        content = content.replace(
            "# import apis into sdk package",
            "# Import compat module first to apply BaseModel patches\nimport graphsense.compat  # noqa: F401\n\n# import apis into sdk package",
        )
        init_path.write_text(content)


# Fields that need CompatInt wrapping (were ModelSimple with int value)
# This includes both 'height' fields and block-related fields that need .to_str()
HEIGHT_FIELDS = ["height", "before_block", "after_block"]

# Fields that need CompatList wrapping (were ModelSimple with list value)
LIST_FIELDS = ["inputs", "outputs", "actors", "tags", "address"]

# Models that contain height field
MODELS_WITH_HEIGHT = [
    "tx_summary",
    "address_tx_utxo",
    "tx_utxo",
    "link_utxo",
    "block",
    "tx_account",
    "rates",
    "block_at_date",
]

# Models that contain list fields
MODELS_WITH_LISTS = {
    "tx_utxo": ["inputs", "outputs"],
    "tx": ["inputs", "outputs"],  # oneOf union
    "address": ["actors"],
    "entity": ["actors"],
    "search_result": ["actors"],
    "search_result_leaf": ["actors"],
    "tx_input": ["address"],
    "tx_output": ["address"],
}


def find_model_files(models_dir: Path) -> dict:
    """Find all model files and return mapping of model name to path."""
    model_files = {}
    for path in models_dir.glob("*.py"):
        if path.name.startswith("__"):
            continue
        # Convert filename to model name (e.g., tx_utxo.py -> tx_utxo)
        model_name = path.stem
        model_files[model_name] = path
    return model_files


def patch_height_field_pydantic_v1(content: str, model_name: str) -> str:
    """
    Patch a model file to wrap height field with CompatInt (Pydantic v1 style).

    For Pydantic v1, we need to:
    1. Import CompatInt
    2. Add a validator that wraps the int value
    """
    if "from pydantic import" not in content:
        return content

    # Check if height field exists
    if not re.search(r"\bheight\s*:", content):
        return content

    print(f"  Patching {model_name} for height field (Pydantic v1)")

    # Add CompatInt import
    if "from graphsense.compat import" not in content:
        # Find pydantic import line and add after it
        content = re.sub(
            r"(from pydantic import[^\n]+)",
            r"\1\nfrom graphsense.compat import CompatInt",
            content,
            count=1,
        )

    # Add validator for height field
    # Look for the class definition and add validator method
    validator_code = '''
    @validator('height', pre=True, always=True)
    def wrap_height_compat(cls, v):
        """Wrap height in CompatInt for backward compatibility."""
        if v is not None and not isinstance(v, CompatInt):
            return CompatInt(v)
        return v
'''

    # Find the class definition and insert validator after field definitions
    # Look for the pattern where fields end and methods begin
    class_match = re.search(r"class\s+\w+\([^)]+\):", content)
    if class_match:
        # Find a good insertion point - after field definitions, before __init__ or at class end
        # Try to find __init__ or Config
        init_match = re.search(r"\n(\s+)def __init__", content)
        config_match = re.search(r"\n(\s+)class Config:", content)

        if init_match:
            insert_pos = init_match.start()
            content = content[:insert_pos] + validator_code + content[insert_pos:]
        elif config_match:
            insert_pos = config_match.start()
            content = content[:insert_pos] + validator_code + content[insert_pos:]

        # Make sure validator is imported
        if (
            "@validator" in content
            and "validator"
            not in content.split("from pydantic import")[1].split("\n")[0]
        ):
            content = content.replace(
                "from pydantic import", "from pydantic import validator, ", 1
            )

    return content


def patch_height_field_pydantic_v2(content: str, model_name: str) -> str:
    """
    Patch a model file to wrap height-related fields with CompatInt (Pydantic v2 style).

    For Pydantic v2, we use field_validator.
    This handles 'height', 'before_block', 'after_block' and similar fields.
    """
    if "from pydantic import" not in content:
        return content

    # Find which HEIGHT_FIELDS exist in this model
    fields_to_patch = []
    for field_name in HEIGHT_FIELDS:
        # Check if field exists and not already patched
        if re.search(rf"\b{field_name}\s*:", content):
            if f"wrap_{field_name}_compat" not in content:
                fields_to_patch.append(field_name)

    if not fields_to_patch:
        return content

    print(
        f"  Patching {model_name} for height-related fields: {fields_to_patch} (Pydantic v2)"
    )

    # Add CompatInt import
    if "from graphsense.compat import" not in content:
        content = re.sub(
            r"(from pydantic import[^\n]+)",
            r"\1\nfrom graphsense.compat import CompatInt",
            content,
            count=1,
        )

    # Make sure field_validator is imported BEFORE adding the validator code
    if (
        "from pydantic import" in content
        and "field_validator" not in content.split("class ")[0]
    ):
        content = content.replace(
            "from pydantic import", "from pydantic import field_validator, ", 1
        )

    # For each field, change type annotation and add validator
    for field_name in fields_to_patch:
        # Change type annotation from StrictInt to int (handles Optional[StrictInt] too)
        content = re.sub(
            rf"{field_name}:\s*Optional\[StrictInt\]",
            f"{field_name}: Optional[int]",
            content,
        )
        content = re.sub(rf"{field_name}:\s*StrictInt\b", f"{field_name}: int", content)

        # Add field_validator for this field using mode='wrap' to preserve CompatInt
        validator_code = f'''
    @field_validator('{field_name}', mode='wrap')
    @classmethod
    def wrap_{field_name}_compat(cls, v, handler):
        """Wrap {field_name} in CompatInt for backward compatibility."""
        validated = handler(v)
        if validated is not None and not isinstance(validated, CompatInt):
            return CompatInt(validated)
        return validated
'''
        # Find insertion point
        class_match = re.search(r"class\s+\w+\([^)]+\):", content)
        if class_match:
            init_match = re.search(r"\n(\s+)def __init__", content)
            config_match = re.search(r"\n(\s+)model_config\s*=", content)

            if init_match:
                insert_pos = init_match.start()
                content = content[:insert_pos] + validator_code + content[insert_pos:]
            elif config_match:
                insert_pos = config_match.start()
                content = content[:insert_pos] + validator_code + content[insert_pos:]

    return content


def patch_list_field_pydantic_v1(content: str, model_name: str, fields: list) -> str:
    """Patch list fields to use CompatList (Pydantic v1 style)."""
    if "from pydantic import" not in content:
        return content

    patched_fields = []
    for field in fields:
        if re.search(rf"\b{field}\s*:", content):
            patched_fields.append(field)

    if not patched_fields:
        return content

    print(f"  Patching {model_name} for list fields: {patched_fields} (Pydantic v1)")

    # Add CompatList import
    if "CompatList" not in content:
        if "from graphsense.compat import" in content:
            content = content.replace(
                "from graphsense.compat import CompatInt",
                "from graphsense.compat import CompatInt, CompatList",
            )
            content = content.replace(
                "from graphsense.compat import",
                "from graphsense.compat import CompatList, ",
            )
        else:
            content = re.sub(
                r"(from pydantic import[^\n]+)",
                r"\1\nfrom graphsense.compat import CompatList",
                content,
                count=1,
            )

    # Add validators for each list field
    for field in patched_fields:
        validator_code = f'''
    @validator('{field}', pre=True, always=True)
    def wrap_{field}_compat(cls, v):
        """Wrap {field} in CompatList for backward compatibility."""
        if v is not None and not isinstance(v, CompatList):
            return CompatList(v) if isinstance(v, list) else v
        return v
'''
        # Find insertion point
        init_match = re.search(r"\n(\s+)def __init__", content)
        config_match = re.search(r"\n(\s+)class Config:", content)

        if init_match:
            insert_pos = init_match.start()
            content = content[:insert_pos] + validator_code + content[insert_pos:]
        elif config_match:
            insert_pos = config_match.start()
            content = content[:insert_pos] + validator_code + content[insert_pos:]

    # Make sure validator is imported
    if "@validator" in content:
        pydantic_import = re.search(r"from pydantic import([^\n]+)", content)
        if pydantic_import and "validator" not in pydantic_import.group(1):
            content = content.replace(
                "from pydantic import", "from pydantic import validator, ", 1
            )

    return content


def patch_list_field_pydantic_v2(content: str, model_name: str, fields: list) -> str:
    """Patch list fields to use CompatList (Pydantic v2 style)."""
    if "from pydantic import" not in content:
        return content

    patched_fields = []
    for field in fields:
        if re.search(rf"\b{field}\s*:", content):
            # Check if this field is already patched
            if f"wrap_{field}_compat" not in content:
                patched_fields.append(field)

    if not patched_fields:
        return content

    print(f"  Patching {model_name} for list fields: {patched_fields} (Pydantic v2)")

    # Add CompatList import
    if "CompatList" not in content:
        if "from graphsense.compat import" in content:
            content = content.replace(
                "from graphsense.compat import CompatInt",
                "from graphsense.compat import CompatInt, CompatList",
            )
        else:
            content = re.sub(
                r"(from pydantic import[^\n]+)",
                r"\1\nfrom graphsense.compat import CompatList",
                content,
                count=1,
            )

    # Make sure field_validator is imported BEFORE adding the validator code
    if (
        "from pydantic import" in content
        and "field_validator" not in content.split("class ")[0]
    ):
        content = content.replace(
            "from pydantic import", "from pydantic import field_validator, ", 1
        )

    # Add field_validators for each list field
    for field in patched_fields:
        validator_code = f'''
    @field_validator('{field}', mode='before')
    @classmethod
    def wrap_{field}_compat(cls, v):
        """Wrap {field} in CompatList for backward compatibility."""
        if v is not None and not isinstance(v, CompatList):
            return CompatList(v) if isinstance(v, list) else v
        return v
'''
        # Find insertion point
        init_match = re.search(r"\n(\s+)def __init__", content)
        config_match = re.search(r"\n(\s+)model_config\s*=", content)

        if init_match:
            insert_pos = init_match.start()
            content = content[:insert_pos] + validator_code + content[insert_pos:]
        elif config_match:
            insert_pos = config_match.start()
            content = content[:insert_pos] + validator_code + content[insert_pos:]

    return content


def detect_pydantic_version(content: str) -> int:
    """Detect which Pydantic version the generated code uses."""
    if "field_validator" in content or "model_config" in content:
        return 2
    return 1


# OneOf union models that need transparent attribute access
ONEOF_MODELS = [
    "tx",
    "link",
    "address_tx",
    "tag",
    "entity",
    "links_inner",
    "location_inner",
]

ONEOF_GETATTR_CODE = '''
    def __getattr__(self, name: str):
        """Delegate attribute access to actual_instance for backward compatibility.

        This allows code like `tx.height` instead of `tx.actual_instance.height`.
        """
        if name.startswith('_') or name in (
            'actual_instance', 'one_of_schemas', 'model_config',
            'discriminator_value_class_map', 'model_fields', 'model_computed_fields',
            'model_extra', 'model_fields_set', 'oneof_schema_1_validator',
            'oneof_schema_2_validator', 'oneof_schema_3_validator'
        ):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        actual = object.__getattribute__(self, 'actual_instance')
        if actual is None:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return getattr(actual, name)

    def __setattr__(self, name: str, value):
        """Delegate attribute setting to actual_instance for backward compatibility."""
        if name.startswith('_') or name in (
            'actual_instance', 'one_of_schemas', 'model_config',
            'discriminator_value_class_map', 'model_fields', 'model_computed_fields',
            'model_extra', 'model_fields_set'
        ):
            super().__setattr__(name, value)
            return

        try:
            actual = object.__getattribute__(self, 'actual_instance')
            if actual is not None:
                setattr(actual, name, value)
                return
        except AttributeError:
            pass

        super().__setattr__(name, value)

    def __getitem__(self, key):
        """Delegate subscript access to actual_instance for backward compatibility.

        This allows code like `tx['height']` instead of `tx.actual_instance['height']`.
        """
        actual = object.__getattribute__(self, 'actual_instance')
        if actual is None:
            raise KeyError(key)
        # Try dict-style access first, then attribute access
        if hasattr(actual, '__getitem__'):
            return actual[key]
        return getattr(actual, key)
'''


def patch_oneof_model(content: str, model_name: str) -> str:
    """Add __getattr__ and __setattr__ to oneOf model for transparent attribute access."""
    # Check if this looks like a oneOf model
    if "actual_instance" not in content:
        return content

    # Check if already patched
    if "def __getattr__" in content:
        return content

    print(f"  Patching {model_name} for transparent oneOf attribute access")

    # Find the class definition end (before any method definitions or at class end)
    # Look for the last field definition or class-level attribute
    class_match = re.search(r"class\s+\w+\([^)]+\):", content)
    if not class_match:
        return content

    # Find a good insertion point - after the validator but before to_json/to_dict methods
    to_json_match = re.search(r"\n(\s+)def to_json\(", content)
    from_json_match = re.search(r"\n(\s+)def from_json\(", content)
    from_dict_match = re.search(r"\n(\s+)@classmethod\s*\n\s+def from_dict\(", content)

    # Insert before from_dict or from_json
    if from_dict_match:
        insert_pos = from_dict_match.start()
    elif from_json_match:
        insert_pos = from_json_match.start()
    elif to_json_match:
        insert_pos = to_json_match.start()
    else:
        # Fallback: insert at end of file before final newlines
        insert_pos = len(content.rstrip())

    content = content[:insert_pos] + ONEOF_GETATTR_CODE + "\n" + content[insert_pos:]
    return content


# Entity model needs special handling: it stores actual_instance as raw dict
# and needs to wrap nested dict values in DictModel for attribute access
ENTITY_GETATTR_CODE = '''
    def __getattr__(self, name):
        """Allow attribute access to entity fields for backward compatibility."""
        # Avoid infinite recursion for private attributes and Pydantic internals
        if name.startswith('_') or name in ('actual_instance', 'anyof_schema_1_validator', 'anyof_schema_2_validator'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        actual = object.__getattribute__(self, 'actual_instance')
        if actual is None:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        if isinstance(actual, dict):
            if name in actual:
                from graphsense.compat import DictModel
                value = actual[name]
                # Wrap nested dicts in DictModel for attribute access
                if isinstance(value, dict):
                    return DictModel(value)
                elif isinstance(value, list):
                    return [DictModel(item) if isinstance(item, dict) else item for item in value]
                return value
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        elif isinstance(actual, int):
            raise AttributeError(f"Cannot access attribute '{name}' on int entity")
        else:
            # For Entity instances
            return getattr(actual, name)
'''


def patch_entity_model(models_dir: Path) -> None:
    """Patch the Entity model specifically for dict-based actual_instance handling."""
    model_files = find_model_files(models_dir)
    if "entity" not in model_files:
        return

    path = model_files["entity"]
    content = path.read_text()

    # Check if already patched with DictModel handling
    if "DictModel" in content:
        return

    # Check if it has our target __getattr__
    if "def __getattr__" not in content:
        return

    print("  Patching Entity model to wrap dict values in DictModel")

    # Replace the existing __getattr__ with one that handles dicts
    old_getattr_pattern = r'''    def __getattr__\(self, name\):
        """Allow attribute access to entity fields for backward compatibility."""
        # Avoid infinite recursion for private attributes and Pydantic internals
        if name\.startswith\('_'\) or name in \('actual_instance', 'anyof_schema_1_validator', 'anyof_schema_2_validator'\):
            raise AttributeError\(f"'{type\(self\)\.__name__}' object has no attribute '{name}'"\)

        actual = object\.__getattribute__\(self, 'actual_instance'\)
        if actual is None:
            raise AttributeError\(f"'{type\(self\)\.__name__}' object has no attribute '{name}'"\)

        if isinstance\(actual, dict\):
            if name in actual:
                return actual\[name\]
            raise AttributeError\(f"'{type\(self\)\.__name__}' object has no attribute '{name}'"\)
        elif isinstance\(actual, int\):
            raise AttributeError\(f"Cannot access attribute '{name}' on int entity"\)
        else:
            # For Entity instances
            return getattr\(actual, name\)'''

    new_getattr = '''    def __getattr__(self, name):
        """Allow attribute access to entity fields for backward compatibility."""
        # Avoid infinite recursion for private attributes and Pydantic internals
        if name.startswith('_') or name in ('actual_instance', 'anyof_schema_1_validator', 'anyof_schema_2_validator'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        actual = object.__getattribute__(self, 'actual_instance')
        if actual is None:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        if isinstance(actual, dict):
            if name in actual:
                from graphsense.compat import DictModel
                value = actual[name]
                # Wrap nested dicts in DictModel for attribute access
                if isinstance(value, dict):
                    return DictModel(value)
                elif isinstance(value, list):
                    return [DictModel(item) if isinstance(item, dict) else item for item in value]
                return value
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        elif isinstance(actual, int):
            raise AttributeError(f"Cannot access attribute '{name}' on int entity")
        else:
            # For Entity instances
            return getattr(actual, name)'''

    new_content = re.sub(old_getattr_pattern, new_getattr, content)
    if new_content != content:
        path.write_text(new_content)


def patch_oneof_models(models_dir: Path) -> None:
    """Patch oneOf models for transparent attribute access."""
    model_files = find_model_files(models_dir)

    for model_name in ONEOF_MODELS:
        if model_name in model_files:
            path = model_files[model_name]
            content = path.read_text()

            new_content = patch_oneof_model(content, model_name)

            if new_content != content:
                path.write_text(new_content)

    # Special patching for Entity model to handle dict-based actual_instance
    patch_entity_model(models_dir)


def patch_api_files(api_dir: Path) -> None:
    """Patch API files to export backward-compatible types like Height."""
    # The old v5 client exported Height from addresses_api
    # We need to add that export for backward compatibility
    addresses_api = api_dir / "addresses_api.py"
    if addresses_api.exists():
        content = addresses_api.read_text()

        if "from graphsense.compat import Height" not in content:
            print("  Adding Height export to addresses_api.py")
            # Add import after other graphsense imports
            content = re.sub(
                r"(from graphsense\.api_client import[^\n]+)",
                r"\1\nfrom graphsense.compat import Height",
                content,
                count=1,
            )
            addresses_api.write_text(content)


def patch_api_client(package_dir: Path) -> None:
    """Patch api_client.py for backward compatibility."""
    api_client = package_dir / "api_client.py"
    if not api_client.exists():
        return

    content = api_client.read_text()
    modified = False

    # Import CompatList at the top of the file
    if "from graphsense.compat import CompatList" not in content:
        print("  Adding CompatList import to api_client.py")
        content = re.sub(
            r"(from graphsense\.configuration import)",
            r"from graphsense.compat import CompatList\n\1",
            content,
            count=1,
        )
        modified = True

    # Wrap list return values in CompatList for backward compatibility (.value support)
    # This happens in the response_deserialize method
    if "CompatList(return_data)" not in content:
        print("  Patching response_deserialize to wrap list results in CompatList")
        # Find the return statement in response_deserialize and add list wrapping
        content = re.sub(
            r"(return ApiResponse\(\s*status_code = response_data\.status,\s*data = )(return_data)(,)",
            r"\1CompatList(return_data) if isinstance(return_data, list) else return_data\3",
            content,
        )
        modified = True

    # Add pool_threads parameter with actual ThreadPoolExecutor support
    if "pool_threads" not in content:
        print(
            "  Adding pool_threads parameter with ThreadPoolExecutor to ApiClient.__init__"
        )
        # Find the __init__ signature and add pool_threads
        content = re.sub(
            r"(def __init__\(\s*self,\s*configuration=None,\s*header_name=None,\s*header_value=None,\s*cookie=None)\s*\)",
            r"\1,\n        pool_threads=1  # Thread pool size for async_req support\n    )",
            content,
        )
        # Add ThreadPoolExecutor import at top
        content = re.sub(
            r"(from graphsense\.compat import)",
            r"from concurrent.futures import ThreadPoolExecutor\n\1",
            content,
            count=1,
        )
        # Add self.pool_threads and thread pool after self.configuration
        content = re.sub(
            r"(self\.configuration = configuration)\n",
            r"""\1
        self.pool_threads = pool_threads
        # Create thread pool for async_req support (v5 compatibility)
        self._thread_pool = ThreadPoolExecutor(max_workers=pool_threads) if pool_threads >= 1 else None
""",
            content,
            count=1,
        )
        modified = True

    # Add thread pool cleanup in __exit__
    if "_thread_pool" in content and "shutdown" not in content:
        print("  Adding thread pool cleanup to __exit__")
        content = re.sub(
            r"(def __exit__\(self, exc_type, exc_value, traceback\):)\s*\n\s*pass",
            r"""\1
        if hasattr(self, '_thread_pool') and self._thread_pool is not None:
            self._thread_pool.shutdown(wait=True)""",
            content,
        )
        modified = True

    # Update existing pool_threads to actually create thread pool (if it was patched before without it)
    if "pool_threads" in content and "_thread_pool" not in content:
        print("  Updating pool_threads to create actual ThreadPoolExecutor")
        # Add ThreadPoolExecutor import
        if "ThreadPoolExecutor" not in content:
            content = re.sub(
                r"(from graphsense\.compat import)",
                r"from concurrent.futures import ThreadPoolExecutor\n\1",
                content,
                count=1,
            )
        # Replace the simple pool_threads assignment with thread pool creation
        content = content.replace(
            "self.pool_threads = pool_threads  # Stored for backward compatibility",
            """self.pool_threads = pool_threads
        # Create thread pool for async_req support (v5 compatibility)
        self._thread_pool = ThreadPoolExecutor(max_workers=pool_threads) if pool_threads >= 1 else None""",
        )
        # Update __exit__ to clean up thread pool
        content = re.sub(
            r"(def __exit__\(self, exc_type, exc_value, traceback\):)\s*\n\s*pass",
            r"""\1
        if hasattr(self, '_thread_pool') and self._thread_pool is not None:
            self._thread_pool.shutdown(wait=True)""",
            content,
        )
        modified = True

    # Fix headers conversion for ApiResponse (HTTPMessage -> dict)
    if (
        "headers = response_data.headers," in content
        and "headers.items()" not in content
    ):
        print("  Fixing headers conversion in ApiResponse")
        content = content.replace(
            "headers = response_data.headers,",
            "headers = dict(response_data.headers.items()) if response_data.headers else None,",
        )
        modified = True

    # Fix boolean serialization to use Python-style True/False for backward compatibility
    if "v = str(v).lower()" in content:
        print("  Fixing boolean serialization (True/False instead of true/false)")
        content = content.replace(
            "v = str(v).lower()",
            "v = str(v)  # Use Python-style True/False for backward compatibility",
        )
        modified = True

    if modified:
        api_client.write_text(content)


def patch_to_dict_null_handling(models_dir: Path) -> None:
    """Patch to_dict() methods to not add None back for nullable dict fields.

    This fixes the issue where user code does `field.get('key', {}).values()`
    but the to_dict() returns {'key': None} instead of omitting the key.
    """
    model_files = find_model_files(models_dir)

    for model_name, path in model_files.items():
        content = path.read_text()

        # Look for the pattern that sets dict fields to None
        # Pattern: if self.field is None and "field" in self.model_fields_set:
        #             _dict['field'] = None
        pattern = r'(\n\s+# set to None if \w+ \(nullable\) is None\n\s+# and model_fields_set contains the field\n\s+if self\.(\w+) is None and "\2" in self\.model_fields_set:\n\s+_dict\[\'\2\'\] = None)'

        matches = list(re.finditer(pattern, content))
        if matches:
            print(
                f"  Patching {model_name} to_dict() to not add None for nullable fields"
            )
            # Remove all these blocks
            for match in reversed(matches):  # Reverse to not mess up positions
                content = content[: match.start()] + content[match.end() :]

            path.write_text(content)


# Models that need positional argument support for backward compatibility
# Order matches v5 API: first argument = first field
MODELS_WITH_POSITIONAL_ARGS = {
    "rate": ["code", "value"],
    "values": [
        "fiat_values",
        "value",
    ],  # GsValue([...rates...], integer) -> fiat_values, value
}


def patch_positional_args(models_dir: Path) -> None:
    """Patch models to accept positional arguments for backward compatibility.

    v5 allowed: Rate("USD", 1.0)
    v7 requires: Rate(code="USD", value=1.0)

    This patches models to accept both forms.
    """
    model_files = find_model_files(models_dir)

    for model_name, fields in MODELS_WITH_POSITIONAL_ARGS.items():
        if model_name not in model_files:
            continue

        path = model_files[model_name]
        content = path.read_text()

        # Check if already patched
        if "_positional_fields" in content:
            continue

        print(f"  Patching {model_name} for positional argument support")

        # Find the model_config line and add positional fields + custom __init__ before it
        positional_init_code = f'''
    _positional_fields: ClassVar[List[str]] = {repr(fields)}

    def __init__(__pydantic_self__, *args, **kwargs):
        """Support positional arguments for backward compatibility with v5."""
        if args:
            for i, arg in enumerate(args):
                if i < len(__pydantic_self__._positional_fields):
                    kwargs[__pydantic_self__._positional_fields[i]] = arg
        super().__init__(**kwargs)
'''
        # Insert before model_config
        content = content.replace(
            "    model_config = ConfigDict(",
            positional_init_code + "\n    model_config = ConfigDict(",
        )

        path.write_text(content)


def patch_async_req_in_existing(api_dir: Path) -> None:
    """Update async_req handling in already-patched API files to use thread pool.

    Previous patches just removed async_req. This updates them to actually use it
    with a thread pool for backward compatibility with v5 async patterns.
    """
    if not api_dir.exists():
        return

    for path in api_dir.glob("*.py"):
        if path.name.startswith("__"):
            continue

        content = path.read_text()

        # Only patch files that already have validate_call_compat
        if "validate_call_compat" not in content:
            continue

        # Check if it has the old async_req handling (just popping it)
        if "_AsyncResult" in content:
            continue  # Already updated

        # Check for the old pattern that just pops async_req
        if "kwargs.pop('async_req', None)" in content:
            print(f"  Updating async_req handling in {path.name} to use thread pool")

            # Add the _AsyncResult class before validate_call_compat
            async_result_class = '''
class _AsyncResult:
    """v5-compatible async result wrapper around concurrent.futures.Future."""
    def __init__(self, future):
        self._future = future

    def get(self, timeout=None):
        """Block and return the result, like v5's ApplyResult.get()."""
        return self._future.result(timeout=timeout)

    def ready(self):
        """Check if the result is ready."""
        return self._future.done()

    def successful(self):
        """Check if the call completed without exception."""
        if not self._future.done():
            return False
        return self._future.exception() is None


'''
            # Insert before validate_call_compat function
            content = content.replace(
                "def validate_call_compat(func):",
                async_result_class + "def validate_call_compat(func):",
            )

            # Replace the old async_req pop with capture
            content = content.replace(
                "kwargs.pop('async_req', None)",
                "async_req = kwargs.pop('async_req', False)",
            )

            # Find the return statement and add async_req handling before it
            # Old pattern: return validated_func(*args, **kwargs)
            old_return = "        return validated_func(*args, **kwargs)"
            new_return = """
        # Handle async_req: submit to thread pool if available
        if async_req:
            # args[0] is self (the API instance), which has api_client
            api_instance = args[0]
            if hasattr(api_instance, 'api_client'):
                thread_pool = getattr(api_instance.api_client, '_thread_pool', None)
                if thread_pool is not None:
                    future = thread_pool.submit(validated_func, *args, **kwargs)
                    return _AsyncResult(future)
                else:
                    import warnings
                    warnings.warn(
                        "async_req=True but no thread pool available (pool_threads=0). "
                        "Running synchronously. Set pool_threads >= 1 for async execution.",
                        UserWarning
                    )
            # No thread pool available, fall through to sync call

        return validated_func(*args, **kwargs)"""

            content = content.replace(old_return, new_return)

            path.write_text(content)


def patch_datetime_format_in_existing(api_dir: Path) -> None:
    """Update datetime format in already-patched API files.

    Fixes the issue where datetime with time/timezone should use ISO 8601 format,
    not just date-only format. This updates files that were already patched with
    the old datetime handling code.
    """
    if not api_dir.exists():
        return

    for path in api_dir.glob("*.py"):
        if path.name.startswith("__"):
            continue

        content = path.read_text()

        # Only patch files that already have validate_call_compat
        if "validate_call_compat" not in content:
            continue

        # Check if it has the old datetime format code (simple strftime without isoformat)
        if "dt.isoformat()" in content:
            continue  # Already updated

        # Check if the old pattern exists
        old_kwargs_pattern = """        # Convert datetime to date string for date parameters (backward compatibility)
        for key in list(kwargs.keys()):
            if 'date' in key.lower() and isinstance(kwargs[key], _dt_compat):
                kwargs[key] = kwargs[key].strftime('%Y-%m-%d')
        # Also check positional args - var_date is typically arg[1]
        args = list(args)
        for i, arg in enumerate(args):
            if isinstance(arg, _dt_compat):
                args[i] = arg.strftime('%Y-%m-%d')"""

        new_kwargs_pattern = """        # Convert datetime to date string for date parameters (backward compatibility)
        # Preserve full ISO 8601 format when datetime has time/timezone info
        for key in list(kwargs.keys()):
            if 'date' in key.lower() and isinstance(kwargs[key], _dt_compat):
                dt = kwargs[key]
                if dt.hour or dt.minute or dt.second or dt.tzinfo:
                    # Preserve full datetime with timezone (ISO 8601)
                    kwargs[key] = dt.isoformat()
                else:
                    # Date-only (midnight, no timezone) - use simple format
                    kwargs[key] = dt.strftime('%Y-%m-%d')
        # Also check positional args - var_date is typically arg[1]
        args = list(args)
        for i, arg in enumerate(args):
            if isinstance(arg, _dt_compat):
                if arg.hour or arg.minute or arg.second or arg.tzinfo:
                    args[i] = arg.isoformat()
                else:
                    args[i] = arg.strftime('%Y-%m-%d')"""

        if old_kwargs_pattern in content:
            print(f"  Updating datetime format in {path.name}")
            content = content.replace(old_kwargs_pattern, new_kwargs_pattern)
            path.write_text(content)


def patch_api_validate_call(api_dir: Path) -> None:
    """Patch API files to accept unknown kwargs (like async_req) for backward compatibility.

    v7 uses @validate_call which rejects unknown kwargs. We modify it to use
    extra='ignore' so old code passing async_req=True continues to work.
    """
    if not api_dir.exists():
        return

    for path in api_dir.glob("*.py"):
        if path.name.startswith("__"):
            continue

        content = path.read_text()

        # Check if this file has @validate_call
        if "@validate_call" not in content:
            continue

        # Check if already patched
        if "validate_call_compat" in content:
            continue

        print(f"  Patching {path.name} to accept unknown kwargs (async_req support)")

        # Add import for functools and custom decorator at the top
        compat_import = '''
# Backward compatibility wrapper for @validate_call to accept async_req
from functools import wraps
from datetime import datetime as _dt_compat
from pydantic import ConfigDict
_validate_call_config = ConfigDict(arbitrary_types_allowed=True)


class _AsyncResult:
    """v5-compatible async result wrapper around concurrent.futures.Future."""
    def __init__(self, future):
        self._future = future

    def get(self, timeout=None):
        """Block and return the result, like v5's ApplyResult.get()."""
        return self._future.result(timeout=timeout)

    def ready(self):
        """Check if the result is ready."""
        return self._future.done()

    def successful(self):
        """Check if the call completed without exception."""
        if not self._future.done():
            return False
        return self._future.exception() is None


def validate_call_compat(func):
    """Wrapper that filters out legacy kwargs like async_req before validation."""
    from pydantic import validate_call as _validate_call
    # Apply validate_call with config that allows arbitrary types
    validated_func = _validate_call(config=_validate_call_config)(func)
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Capture async_req before removing it
        async_req = kwargs.pop('async_req', False)
        kwargs.pop('_preload_content', None)
        kwargs.pop('_return_http_data_only', None)
        # Convert datetime to date string for date parameters (backward compatibility)
        # Preserve full ISO 8601 format when datetime has time/timezone info
        for key in list(kwargs.keys()):
            if 'date' in key.lower() and isinstance(kwargs[key], _dt_compat):
                dt = kwargs[key]
                if dt.hour or dt.minute or dt.second or dt.tzinfo:
                    # Preserve full datetime with timezone (ISO 8601)
                    kwargs[key] = dt.isoformat()
                else:
                    # Date-only (midnight, no timezone) - use simple format
                    kwargs[key] = dt.strftime('%Y-%m-%d')
        # Also check positional args - var_date is typically arg[1]
        args = list(args)
        for i, arg in enumerate(args):
            if isinstance(arg, _dt_compat):
                if arg.hour or arg.minute or arg.second or arg.tzinfo:
                    args[i] = arg.isoformat()
                else:
                    args[i] = arg.strftime('%Y-%m-%d')

        # Handle async_req: submit to thread pool if available
        if async_req:
            # args[0] is self (the API instance), which has api_client
            api_instance = args[0]
            if hasattr(api_instance, 'api_client'):
                thread_pool = getattr(api_instance.api_client, '_thread_pool', None)
                if thread_pool is not None:
                    future = thread_pool.submit(validated_func, *args, **kwargs)
                    return _AsyncResult(future)
                else:
                    import warnings
                    warnings.warn(
                        "async_req=True but no thread pool available (pool_threads=0). "
                        "Running synchronously. Set pool_threads >= 1 for async execution.",
                        UserWarning
                    )
            # No thread pool available, fall through to sync call

        return validated_func(*args, **kwargs)
    return wrapper
'''
        # Insert after the initial imports
        import_section_end = content.find("\nclass ")
        if import_section_end > 0:
            # Find last import line before class
            last_import = 0
            for match in re.finditer(
                r"^(from|import)\s+", content[:import_section_end], re.MULTILINE
            ):
                last_import = content.find("\n", match.end())

            if last_import > 0:
                content = (
                    content[: last_import + 1]
                    + compat_import
                    + content[last_import + 1 :]
                )

        # Replace @validate_call with @validate_call_compat
        content = re.sub(r"@validate_call\n", "@validate_call_compat\n", content)

        # Remove original validate_call import since we define our own
        content = re.sub(
            r"from pydantic import validate_call, ", "from pydantic import ", content
        )
        content = re.sub(r", validate_call\b", "", content)
        content = re.sub(r"from pydantic import validate_call\n", "", content)

        path.write_text(content)


def patch_api_datetime_params(api_dir: Path) -> None:
    """Patch API methods to accept datetime objects for date parameters.

    User code passes datetime objects to methods like get_block_by_date(),
    but v7 expects StrictStr. We add conversion in the method.
    """
    if not api_dir.exists():
        return

    blocks_api = api_dir / "blocks_api.py"
    if not blocks_api.exists():
        return

    content = blocks_api.read_text()

    # Check if already patched
    if "datetime" in content and "isinstance" in content and "var_date" in content:
        return

    print("  Patching blocks_api.py to accept datetime for date parameters")

    # Add datetime import
    if "from datetime import datetime" not in content:
        content = re.sub(
            r"(from typing import)",
            r"from datetime import datetime as _datetime\n\1",
            content,
            count=1,
        )

    # Patch get_block_by_date methods to convert datetime to string
    # Find the serialize method and add conversion
    serialize_func = "_get_block_by_date_serialize"

    # Add conversion at the start of the serialize method
    conversion_code = """
        # Convert datetime to string for backward compatibility
        if isinstance(var_date, _datetime):
            var_date = var_date.strftime('%Y-%m-%d')
"""
    pattern = rf"(def {serialize_func}\([^)]+\)[^:]*:\s*\n)"
    content = re.sub(pattern, r"\1" + conversion_code, content)

    # Also need to change the type annotation to accept Union[str, datetime]
    # Change var_date: Annotated[StrictStr, ...] to accept datetime too
    content = re.sub(
        r"var_date: Annotated\[StrictStr,",
        r"var_date: Annotated[Union[StrictStr, _datetime],",
        content,
    )

    blocks_api.write_text(content)


def patch_model_files(models_dir: Path) -> None:
    """Patch model files to use compat types."""
    model_files = find_model_files(models_dir)

    # Patch models with height field
    for model_name in MODELS_WITH_HEIGHT:
        if model_name in model_files:
            path = model_files[model_name]
            content = path.read_text()
            pydantic_version = detect_pydantic_version(content)

            if pydantic_version == 2:
                new_content = patch_height_field_pydantic_v2(content, model_name)
            else:
                new_content = patch_height_field_pydantic_v1(content, model_name)

            if new_content != content:
                path.write_text(new_content)

    # Patch models with list fields
    for model_name, fields in MODELS_WITH_LISTS.items():
        if model_name in model_files:
            path = model_files[model_name]
            content = path.read_text()
            pydantic_version = detect_pydantic_version(content)

            if pydantic_version == 2:
                new_content = patch_list_field_pydantic_v2(content, model_name, fields)
            else:
                new_content = patch_list_field_pydantic_v1(content, model_name, fields)

            if new_content != content:
                path.write_text(new_content)


def main():
    if len(sys.argv) < 2:
        print("Usage: patch_compat.py <client_directory>")
        print("  client_directory: Path to the generated Python client")
        sys.exit(1)

    client_dir = Path(sys.argv[1])
    if not client_dir.exists():
        print(f"Error: Directory {client_dir} does not exist")
        sys.exit(1)

    # Find the package directory (graphsense/)
    package_dir = client_dir / "graphsense"
    if not package_dir.exists():
        print(f"Error: Package directory {package_dir} does not exist")
        sys.exit(1)

    models_dir = package_dir / "models"
    if not models_dir.exists():
        # Try model/ instead of models/
        models_dir = package_dir / "model"

    if not models_dir.exists():
        print(f"Error: Models directory not found in {package_dir}")
        sys.exit(1)

    print(f"Patching generated client in {client_dir}")
    print(f"Package directory: {package_dir}")
    print(f"Models directory: {models_dir}")

    # Create compat module
    create_compat_module(package_dir)

    # Patch package __init__.py to import compat early
    patch_package_init(package_dir)

    # Patch model __init__.py
    patch_model_init(models_dir)

    # Patch model files for compat types (.value support)
    patch_model_files(models_dir)

    # Patch oneOf models for transparent attribute access
    patch_oneof_models(models_dir)

    # Patch API files to export backward-compatible types
    api_dir = package_dir / "api"
    if api_dir.exists():
        patch_api_files(api_dir)

    # Patch api_client.py for backward compatibility (pool_threads, etc.)
    patch_api_client(package_dir)

    # Patch to_dict() methods to not add None for nullable fields
    patch_to_dict_null_handling(models_dir)

    # Patch models to accept positional arguments (Rate, Values)
    patch_positional_args(models_dir)

    # Patch API methods to accept async_req kwarg
    if api_dir.exists():
        patch_api_validate_call(api_dir)

    # Update datetime format in already-patched API files (ISO 8601 for datetime with time/tz)
    if api_dir.exists():
        patch_datetime_format_in_existing(api_dir)

    # Update async_req handling in already-patched API files to use thread pool
    if api_dir.exists():
        patch_async_req_in_existing(api_dir)

    # Patch API methods to accept datetime for date parameters
    if api_dir.exists():
        patch_api_datetime_params(api_dir)

    print("Done!")


if __name__ == "__main__":
    main()
