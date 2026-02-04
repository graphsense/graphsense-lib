# Python Client Compatibility Layer

This directory contains the backward compatibility layer for the OpenAPI v7 generated Python client.

## Purpose

The v7 OpenAPI generator produces clients with breaking changes from v5:
- `@validate_call` rejects unknown kwargs like `async_req`
- Union types (oneOf/anyOf) require `.actual_instance` access
- ModelSimple wrappers removed (no more `.value` on Height, TxInputs, etc.)
- Pydantic v2 models don't support dict-style access

This compatibility layer patches the generated client to maintain backward compatibility with existing user code.

## Files

- `patch_compat.py` - Post-processing script that patches the generated client
- `test_compat.py` - Comprehensive tests for backward compatibility
- `README.md` - This file

## Usage

The patch script is called automatically by the Makefile after code generation:

```bash
cd clients/python
make generate-openapi-client
```

To run just the compatibility tests:

```bash
python clients/python-compat/test_compat.py
# Or specify a client directory:
python clients/python-compat/test_compat.py clients/python
```

## What Gets Patched

### 1. Compat Types (`compat.py`)

- **CompatInt**: Integer subclass with `.value` property and arithmetic that preserves type
- **CompatList**: List subclass with `.value` property
- **DictModel**: Dict wrapper with attribute access for oneOf/anyOf models
- **Height**: Alias for CompatInt

### 2. Model Files

- Height fields (`height`, `before_block`, `after_block`) wrapped in CompatInt
- List fields (`inputs`, `outputs`, `actors`, `address`) wrapped in CompatList
- OneOf models get transparent `__getattr__` delegation to `actual_instance`

### 3. API Files

- `@validate_call` replaced with `@validate_call_compat` that:
  - Accepts `async_req`, `_preload_content`, `_return_http_data_only` kwargs
  - Converts datetime to ISO 8601 strings for date parameters
  - Submits to thread pool when `async_req=True`

### 4. ApiClient

- `pool_threads` parameter creates ThreadPoolExecutor for async support
- List responses wrapped in CompatList
- Headers properly converted to dict

## Features

### Arithmetic Preservation (Fix 3)
```python
h = CompatInt(100)
result = h + 1  # Returns CompatInt(101), not plain int
result.value    # Still works: 101
```

### Dict Protocol (Fix 2)
```python
d = DictModel({'a': 1, 'b': 2})
'a' in d        # True
len(d)          # 2
list(d.keys())  # ['a', 'b']
```

### Serialization (Fix 4)
```python
import pickle, copy
h = CompatInt(100)
pickle.loads(pickle.dumps(h))  # CompatInt(100)
copy.deepcopy(h)               # CompatInt(100)
```

### Caching (Fix 5)
```python
d = DictModel({'nested': {'x': 1}})
d.nested is d.nested  # True (cached)
```

### Thread Pool (Fix 1)
```python
# Default pool_threads=1 now creates a thread pool
with ApiClient(config, pool_threads=1) as client:
    api = AddressesApi(client)
    result = api.get_address("btc", "addr", async_req=True)
    data = result.get()  # Blocks until complete
```

## Known Limitations

- Nested lists in models are not wrapped in CompatList (only top-level)
- Thread pool warning when `async_req=True` but `pool_threads=0`
