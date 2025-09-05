import asyncio
import logging
import httpx
from typing import Optional

logger = logging.getLogger(__name__)


class RetryHTTPClient:
    def __init__(self, max_retries: int = 3, timeout: float = 10.0):
        self.max_retries = max_retries
        self.timeout = timeout

    async def get(self, url: str, **kwargs) -> Optional[httpx.Response]:
        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(url, timeout=self.timeout, **kwargs)
                    if response.status_code < 500:  # Don't retry client errors
                        return response
                    elif attempt < self.max_retries - 1:
                        wait_time = 2**attempt
                        logger.warning(
                            f"Server error {response.status_code}, retrying in {wait_time}s..."
                        )
                        await asyncio.sleep(wait_time)
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Connection error: {e}, retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning(f"HTTP error after {self.max_retries} attempts: {e}")
                    return None
        return None
