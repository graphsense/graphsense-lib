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
