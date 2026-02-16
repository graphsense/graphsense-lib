"""GraphSense REST API - FastAPI Application

This module provides the FastAPI application factory.
"""

from graphsenselib.web.app import create_app

__all__ = ["create_app"]
