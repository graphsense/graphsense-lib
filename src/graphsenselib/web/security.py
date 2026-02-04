"""API Security configuration for FastAPI.

This module defines the API key security scheme used throughout the API.
The security scheme is named 'api_key' for backward compatibility with
the existing Python client generator.
"""

from typing import Optional

from fastapi import Security
from fastapi.security import APIKeyHeader

# API Key security scheme
# scheme_name="api_key" ensures the OpenAPI spec uses "api_key" as the scheme name
# for backward compatibility with the existing Python client
api_key_header = APIKeyHeader(
    name="Authorization",
    scheme_name="api_key",
    auto_error=False,
    description="API key for authentication",
)


async def get_api_key(
    api_key: Optional[str] = Security(api_key_header),
) -> Optional[str]:
    """Dependency to extract API key from header.

    This doesn't validate the key - that's handled by the API gateway/proxy.
    This just ensures the security scheme appears in the OpenAPI spec.
    """
    return api_key
