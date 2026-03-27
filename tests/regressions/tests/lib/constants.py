"""Shared constants for regression test infrastructure."""

# --- Container images ---
VANILLA_CASSANDRA_IMAGE = "cassandra:4.1.4"
MINIO_IMAGE = "minio/minio:latest"

# --- MinIO defaults ---
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_CONTAINER_PORT = 9000
MINIO_HEALTH_TIMEOUT_S = 30

# --- Subprocess defaults ---
INGEST_TIMEOUT_S = 600
TRANSFORMATION_TIMEOUT_S = 1200

# --- Default reference version ---
DEFAULT_REF_VERSION = "v25.11.18"
