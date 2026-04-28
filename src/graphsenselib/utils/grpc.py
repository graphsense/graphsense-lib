from grpc import metadata_call_credentials
import grpc
import json

from urllib.parse import urlparse, urlunparse


def remove_credentials_from_url(url):
    scheme, netloc, path, params, query, fragment = urlparse(url)
    netloc = netloc.split("@")[-1]
    return urlunparse((scheme, netloc, path, params, query, fragment))


def get_channel(url: str, enable_retries: bool = True):
    """
    Creates a gRPC channel for the given URL.

    Args:
        url: The gRPC endpoint URL (grpc:// or grpcs://)
        enable_retries: If True, enables automatic retries with exponential backoff

    Returns:
        grpc.Channel: A gRPC channel
    """
    options = []

    if enable_retries:
        service_config = get_retry_service_config()
        options.extend(
            [
                ("grpc.enable_retries", 1),
                ("grpc.service_config", service_config),
                ("grpc.max_retry_attempts", 9),
                ("grpc.keepalive_time_ms", 30000),
                ("grpc.keepalive_timeout_ms", 5000),
                ("grpc.keepalive_permit_without_calls", True),
                ("grpc.http2.max_pings_without_data", 0),
                ("grpc.http2.min_time_between_pings_ms", 10000),
                ("grpc.http2.min_ping_interval_without_data_ms", 300000),
            ]
        )

    parsed_url = urlparse(url)
    if parsed_url.scheme == "grpc":
        url_without_credentials = f"{parsed_url.hostname}:{parsed_url.port}"
        return grpc.insecure_channel(
            url_without_credentials, options=options if options else None
        )
    elif parsed_url.scheme == "grpcs":
        if parsed_url.username != "x-token":
            raise ValueError(
                "For grpcs scheme, username must be 'x-token': using it to indicate token-based auth."
            )

        api_token = parsed_url.password
        if not api_token:
            raise ValueError("For grpcs scheme, password must be the API token.")

        # Define a call credentials function
        def token_credentials(context, callback):
            callback([("x-token", api_token)], None)

        # Create call credentials
        call_credentials = metadata_call_credentials(token_credentials)

        # If you also have SSL credentials, compose them
        channel_credentials = grpc.ssl_channel_credentials()
        composed_credentials = grpc.composite_channel_credentials(
            channel_credentials, call_credentials
        )

        # use parsed_url.netloc without credentials
        url_without_credentials = f"{parsed_url.hostname}:{parsed_url.port}"
        return grpc.secure_channel(
            url_without_credentials,
            composed_credentials,
            options=options if options else None,
        )
    else:
        raise ValueError(f"Unsupported URL scheme: {parsed_url.scheme}")


def get_retry_service_config():
    """
    Returns a gRPC service configuration with retry policy for RESOURCE_EXHAUSTED.

    Total backoff window covers ~5 minutes so calls survive a node restart:
    3+6+12+24+48+60+60+60 ≈ 273s across 9 attempts.
    """
    return json.dumps(
        {
            "methodConfig": [
                {
                    "name": [{}],  # Apply to all methods
                    "retryPolicy": {
                        "maxAttempts": 9,
                        "initialBackoff": "3s",
                        "maxBackoff": "60s",
                        "backoffMultiplier": 2.0,
                        "retryableStatusCodes": ["RESOURCE_EXHAUSTED", "UNAVAILABLE"],
                    },
                }
            ]
        }
    )
