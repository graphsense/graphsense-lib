import importlib
import asyncio
import json
import logging
import logging.handlers
import os
import re
import traceback
from contextlib import asynccontextmanager, suppress
from html import escape
from typing import Any, Optional

import yaml
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError as PydanticValidationError
from pydantic_core import ValidationError as PydanticCoreValidationError
from graphsenselib.config import AppConfig
from graphsenselib.config.tagstore_config import set_tagstore_max_concurrency
from graphsenselib.web.version import __api_version__
from graphsenselib.db.asynchronous.services.tags_service import ConceptProtocol
from graphsenselib.errors import (
    BadUserInputException,
    FeatureNotAvailableException,
    GsTimeoutException,
    NotFoundException,
)
from graphsenselib.tagstore.db import TagstoreDbAsync, Taxonomies
from graphsenselib.tagstore.db.database import (
    ensure_database_initialized,
    get_db_engine_async,
)
from graphsenselib.utils.slack import SlackLogHandler

from graphsenselib.web.builtin.plugins.obfuscate_tags.obfuscate_tags import (
    ObfuscateTags,
)
from graphsenselib.web.config import GSRestConfig, LoggingConfig
from graphsenselib.web.dependencies import MockTagstoreDb, ServiceContainer
from graphsenselib.web.middleware.deprecation import DeprecationHeaderMiddleware
from graphsenselib.web.middleware.empty_params import EmptyQueryParamsMiddleware
from graphsenselib.web.middleware.plugins import PluginMiddleware
from graphsenselib.web.plugins import get_subclass
from graphsenselib.web.routes import (
    addresses,
    blocks,
    bulk,
    clusters,
    entities,
    general,
    rates,
    tags,
    tokens,
    txs,
)
from graphsenselib.web.security import get_api_key

CONFIG_FILE = "./instance/config.yaml"
DOCS_STATIC_DIR = "./docs/static"
DOCS_STATIC_URL = "/docs_assets"
DEFAULT_DOCS_LOGO_URL = f"{DOCS_STATIC_URL}/logo.png"
DEFAULT_DOCS_FAVICON_ICO_URL = f"{DOCS_STATIC_URL}/favicon.ico"
DEFAULT_DOCS_FAVICON_PNG_URL = f"{DOCS_STATIC_URL}/favicon.png"
API_DESCRIPTION = """\
GraphSense API provides programmatic access to blockchain analytics data across
multiple ledgers. Use it to explore addresses, clusters, blocks, transactions,
tags, token activity, and exchange-rate context, and to integrate investigation
workflows into your own applications and automation.

## Versioning and deprecation policy

The API follows semantic versioning. Minor releases are additive and
backwards-compatible; breaking changes only happen in major releases, which
are rare and announced in advance.

Deprecated endpoints and fields remain fully functional for at least six
months after they are marked deprecated. During that window they are
highlighted with a strikethrough in the docs and in generated clients, and
responses from deprecated endpoints carry a `Deprecation` HTTP header that
client tooling can detect. Replacement endpoints and fields are always
introduced before the deprecated surface is removed.

See the [full versioning and deprecation policy](https://github.com/graphsense/graphsense-lib/blob/master/README.md#rest-api-evolution-and-deprecation-policy)
for details.
"""
logger = logging.getLogger(__name__)


def _to_snake_case(name: str) -> str:
    """Convert PascalCase or camelCase to snake_case.

    This is used to generate backward-compatible OpenAPI schema names
    that match the original Connexion-based API.
    """
    # Insert underscore before uppercase letters (except at start)
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Insert underscore before uppercase letters that follow lowercase
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _add_missing_union_schemas(schema: dict[str, Any]) -> dict[str, Any]:
    """Add missing union schemas for backward compatibility with client generator.

    The original Connexion-based API had explicit union schemas like 'tx', 'link',
    'address_tx' that the Python client generator uses. FastAPI inlines these as
    anyOf in responses, but we need the named schemas for client compatibility.
    """
    schemas = schema.get("components", {}).get("schemas", {})
    if not schemas:
        return schema

    # Define the union schemas that need to be added
    # These match the original OpenAPI spec structure
    union_schemas = {
        "tx": {
            "title": "tx",
            "discriminator": {
                "propertyName": "tx_type",
                "mapping": {
                    "utxo": "#/components/schemas/tx_utxo",
                    "account": "#/components/schemas/tx_account",
                },
            },
            "oneOf": [
                {"$ref": "#/components/schemas/tx_utxo"},
                {"$ref": "#/components/schemas/tx_account"},
            ],
        },
        "link": {
            "title": "link",
            "discriminator": {
                "propertyName": "tx_type",
                "mapping": {
                    "utxo": "#/components/schemas/link_utxo",
                    "account": "#/components/schemas/tx_account",
                },
            },
            "oneOf": [
                {"$ref": "#/components/schemas/link_utxo"},
                {"$ref": "#/components/schemas/tx_account"},
            ],
        },
        "address_tx": {
            "title": "address_tx",
            "discriminator": {
                "propertyName": "tx_type",
                "mapping": {
                    "utxo": "#/components/schemas/address_tx_utxo",
                    "account": "#/components/schemas/tx_account",
                },
            },
            "oneOf": [
                {"$ref": "#/components/schemas/address_tx_utxo"},
                {"$ref": "#/components/schemas/tx_account"},
            ],
        },
        "tag": {
            "title": "tag",
            "type": "object",
            "properties": {
                "label": {"type": "string"},
                "category": {"type": "string"},
                "abuse": {"type": "string"},
                "actor": {"type": "string"},
                "concepts": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/concept"},
                },
            },
        },
    }

    # Add missing schemas
    for name, definition in union_schemas.items():
        if name not in schemas:
            schemas[name] = definition

    return schema


def _fix_response_schemas(schema: dict[str, Any]) -> dict[str, Any]:
    """Fix response schemas by replacing inline anyOf with refs to named union schemas.

    This function replaces inline anyOf union types with $ref to named union schemas
    that the Python client generator expects.
    """
    # Define the mapping from anyOf patterns to union schema refs
    anyof_to_ref = {
        # tx union: tx_utxo | tx_account
        frozenset(
            ["#/components/schemas/tx_utxo", "#/components/schemas/tx_account"]
        ): "#/components/schemas/tx",
        frozenset(
            ["#/components/schemas/TxUtxo", "#/components/schemas/TxAccount"]
        ): "#/components/schemas/tx",
        # link union: link_utxo | tx_account
        frozenset(
            ["#/components/schemas/link_utxo", "#/components/schemas/tx_account"]
        ): "#/components/schemas/link",
        frozenset(
            ["#/components/schemas/LinkUtxo", "#/components/schemas/TxAccount"]
        ): "#/components/schemas/link",
        # address_tx union: address_tx_utxo | tx_account
        frozenset(
            ["#/components/schemas/address_tx_utxo", "#/components/schemas/tx_account"]
        ): "#/components/schemas/address_tx",
        frozenset(
            ["#/components/schemas/AddressTxUtxo", "#/components/schemas/TxAccount"]
        ): "#/components/schemas/address_tx",
    }

    def fix_schema(obj: Any) -> Any:
        if isinstance(obj, dict):
            # Check if this is an anyOf that should be replaced with a union ref
            if "anyOf" in obj and isinstance(obj["anyOf"], list):
                anyof_items = obj["anyOf"]

                # Check for union type refs
                refs = set()
                for item in anyof_items:
                    if isinstance(item, dict) and "$ref" in item:
                        refs.add(item["$ref"])
                frozen_refs = frozenset(refs)
                if frozen_refs in anyof_to_ref:
                    # Replace with $ref to union schema
                    return {"$ref": anyof_to_ref[frozen_refs]}

            # Recursively process nested structures
            return {k: fix_schema(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [fix_schema(item) for item in obj]
        return obj

    return fix_schema(schema)


def _promote_schema_examples_to_parameter_level(
    schema: dict[str, Any],
) -> dict[str, Any]:
    """Move examples from schema.examples to parameter-level example.

    FastAPI with Pydantic v2 puts examples from Path(examples=[...]) and
    Query(examples=[...]) inside the parameter's JSON Schema as schema.examples
    (an array). Swagger UI does not read this field — it only reads the
    parameter-level 'example' field (OpenAPI 3.0) or 'examples' map (OpenAPI 3.1).

    This post-processor promotes schema.examples[0] to parameter.example so
    that Swagger UI displays them correctly.
    """

    def _to_python_literal(value: Any) -> str:
        if value is None:
            return "None"
        if isinstance(value, bool):
            return "True" if value else "False"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            return repr(value)
        return repr(value)

    for path_val in schema.get("paths", {}).values():
        for method_val in path_val.values():
            if not isinstance(method_val, dict):
                continue
            for param in method_val.get("parameters", []):
                param_schema = param.get("schema", {})
                if "examples" in param_schema and "example" not in param:
                    examples = param_schema.pop("examples")
                    if isinstance(examples, list) and examples:
                        param["example"] = examples[0]
                        param["x-graphsense-python-example"] = _to_python_literal(
                            examples[0]
                        )
    return schema


def _normalize_parameter_examples_for_generated_clients(
    schema: dict[str, Any],
) -> dict[str, Any]:
    """Normalize selected OpenAPI parameter examples for generated client snippets.

    Some generators eagerly include all optional parameters in example code. For a
    few parameters this yields invalid/default placeholder combinations in snippets.
    Keep these examples explicit and safe at the OpenAPI source.
    """

    for path_item in schema.get("paths", {}).values():
        for operation in path_item.values():
            if not isinstance(operation, dict):
                continue

            parameters = operation.get("parameters", [])
            parameter_names = {p.get("name") for p in parameters if isinstance(p, dict)}
            has_height_filters = (
                "min_height" in parameter_names or "max_height" in parameter_names
            )

            for parameter in parameters:
                if not isinstance(parameter, dict):
                    continue

                name = parameter.get("name")

                if name in {"page", "token_tx_id"}:
                    parameter["example"] = None
                    parameter.pop("x-graphsense-python-example", None)
                    continue

                if has_height_filters and name in {"min_date", "max_date"}:
                    parameter["example"] = None
                    parameter.pop("x-graphsense-python-example", None)

    return schema


def _convert_schema_names_to_snake_case(schema: dict[str, Any]) -> dict[str, Any]:
    """Post-process OpenAPI schema to use snake_case schema names.

    This ensures backward compatibility with the original Connexion-based API
    which used snake_case schema names (e.g., 'address_tag' instead of 'AddressTag').
    The Python client generator depends on these schema names.
    """
    # First add missing union schemas
    schema = _add_missing_union_schemas(schema)

    # Fix response schemas to use refs instead of inline anyOf
    schema = _fix_response_schemas(schema)

    # Serialize to JSON and replace all $ref occurrences
    schema_json = json.dumps(schema)

    # Get all schema names from components
    schemas = schema.get("components", {}).get("schemas", {})
    if not schemas:
        return schema

    # Build mapping from PascalCase to snake_case
    # Only convert our custom models, not FastAPI built-in schemas like HTTPValidationError
    builtin_schemas = {"HTTPValidationError", "ValidationError"}
    name_mapping = {}
    for name in schemas.keys():
        if name not in builtin_schemas:
            snake_name = _to_snake_case(name)
            if snake_name != name:
                name_mapping[name] = snake_name

    # Replace all occurrences in the JSON
    for old_name, new_name in name_mapping.items():
        # Replace in $ref paths: "#/components/schemas/OldName" -> "#/components/schemas/new_name"
        schema_json = schema_json.replace(
            f'"#/components/schemas/{old_name}"', f'"#/components/schemas/{new_name}"'
        )

    # Parse back to dict
    schema = json.loads(schema_json)

    # Rename the schema keys themselves
    if "components" in schema and "schemas" in schema["components"]:
        old_schemas = schema["components"]["schemas"]
        new_schemas = {}
        for name, definition in old_schemas.items():
            new_name = name_mapping.get(name, name)
            new_schemas[new_name] = definition
        schema["components"]["schemas"] = new_schemas

    return schema


def load_config(config_file: str) -> dict:
    if not os.path.exists(config_file):
        raise ValueError(f"Config file {config_file} not found.")

    with open(config_file, "r") as input_file:
        config = yaml.safe_load(input_file)
    return config


def setup_logging(
    app_logger,
    slack_exception_hook,
    default_environment: Optional[str],
    logging_config: LoggingConfig,
):
    level = logging_config.level.upper()
    level = getattr(logging, level)
    FORMAT = "%(asctime)s %(message)s"
    logging.basicConfig(format=FORMAT)
    app_logger.setLevel(level)

    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("cassandra").setLevel(logging.INFO)

    for handler in logging.root.handlers:
        handler.setFormatter(
            logging.Formatter(
                "%(levelname)-8s %(asctime)s "
                "%(name)s:%(filename)s:%(lineno)d %(message)s"
            )
        )

    if slack_exception_hook is not None:
        for h in slack_exception_hook.hooks:
            slack_handler = SlackLogHandler(h, environment=default_environment)
            slack_handler.setLevel("ERROR")
            app_logger.addHandler(slack_handler)

    smtp = logging_config.smtp
    if not smtp:
        return

    credentials = None
    secure = None
    if smtp.username is not None:
        credentials = (smtp.username, smtp.password)
        if smtp.secure is True:
            secure = ()

    handler = logging.handlers.SMTPHandler(
        mailhost=(smtp.host, smtp.port),
        fromaddr=smtp.from_addr,
        toaddrs=smtp.to,
        subject=smtp.subject,
        credentials=credentials,
        secure=secure,
        timeout=smtp.timeout,
    )

    handler.setLevel(getattr(logging, smtp.level))
    app_logger.addHandler(handler)


def log_slack_exception_notification_status(
    slack_exception_hook, default_environment: Optional[str]
):
    """Log whether Slack exception notifications are configured at startup."""
    hooks = slack_exception_hook.hooks if slack_exception_hook is not None else []
    environment = default_environment or "unknown"

    if hooks:
        logger.info(
            "Slack exception notifications enabled (hooks=%d, environment=%s)",
            len(hooks),
            environment,
        )
    else:
        logger.info(
            "Slack exception notifications disabled (no 'exceptions' slack hooks configured, environment=%s)",
            environment,
        )


async def setup_database(app: FastAPI):
    """Setup database connections (Cassandra + TagStore)"""
    config = app.state.config
    db_config = config.database
    driver = db_config.driver.lower()
    logger.info(f"Opening {driver} connection ...")

    mod = importlib.import_module("graphsenselib.db.asynchronous." + driver)
    cls = getattr(mod, driver.capitalize())
    app.state.db = cls(db_config, logger)

    ts_conf = config.tagstore

    def _activate_mock_tagstore(reason: str):
        logger.warning(
            "TagStore unavailable (%s). Falling back to mock TagStore.",
            reason,
        )
        app.state.tagstore_engine = None
        app.state.tagstore_db = MockTagstoreDb()
        ConceptsCacheServiceFastAPI.setup_empty_cache(app)

    if ts_conf is None or not getattr(ts_conf, "url", None):
        _activate_mock_tagstore("configuration missing")
        logger.info("Database setup done")
        return

    engine = None
    initialized = False
    try:
        if config.ensure_tagstore_schema_on_startup:
            logger.info(
                "TagStore schema auto-init is enabled; checking required tables/views"
            )
            initialized = await asyncio.to_thread(
                ensure_database_initialized,
                ts_conf.url,
                False,
            )

        max_conn = ts_conf.pool_size
        max_pool_time = ts_conf.pool_timeout
        mo = ts_conf.max_overflow
        recycle = ts_conf.pool_recycle
        enable_prepared_statements_cache = ts_conf.enable_prepared_statements_cache

        # Activate the configured fan-out cap. The capacity invariant
        # (pool_size + max_overflow >= max_concurrency) is enforced by the
        # TagStoreReaderConfig model_validator, so by the time we reach this
        # branch the value is known-safe.
        set_tagstore_max_concurrency(ts_conf.max_concurrency)
        logger.info(
            "TagStore fan-out cap: max_concurrency=%d (pool_size=%d, max_overflow=%d)",
            ts_conf.max_concurrency,
            max_conn,
            mo,
        )

        engine = get_db_engine_async(
            ts_conf.url
            + (
                "?prepared_statement_cache_size=0"
                if not enable_prepared_statements_cache
                else ""
            ),
            pool_size=int(max_conn),
            max_overflow=int(mo),
            pool_recycle=int(recycle),
            pool_timeout=int(max_pool_time),
            pool_pre_ping=True,
        )

        tagstore_db = TagstoreDbAsync(engine)
        await ConceptsCacheServiceFastAPI.setup_cache(tagstore_db, app)

        app.state.tagstore_engine = engine
        app.state.tagstore_db = tagstore_db

        if config.ensure_tagstore_schema_on_startup:
            if initialized:
                logger.info("TagStore schema initialized during REST startup")
            else:
                logger.info("TagStore schema already initialized")
    except Exception as exc:
        if engine is not None:
            with suppress(Exception):
                await engine.dispose()
        logger.warning("TagStore startup failed: %s", exc, exc_info=True)
        _activate_mock_tagstore("URL unreachable or initialization failed")
        logger.info("Database setup done")
        return

    logger.info("Database setup done")


async def teardown_database(app: FastAPI):
    """Cleanup database connections"""
    logger.info("Begin app teardown")
    driver = app.state.config.database.driver.lower()
    app.state.db.close()
    logger.info(f"Closed {driver} connection.")
    tagstore_engine = getattr(app.state, "tagstore_engine", None)
    if tagstore_engine is not None:
        with suppress(Exception):
            logger.info(tagstore_engine.pool.status())
        await tagstore_engine.dispose()
        with suppress(Exception):
            logger.info(tagstore_engine.pool.status())
        logger.info("Closed Tagstore connection.")
    else:
        logger.info("TagStore mock active; no TagStore connection to close.")

    # Close Redis client if it exists
    if getattr(app.state, "redis_client", None):
        await app.state.redis_client.aclose()
        logger.info("Closed Redis connection.")


class ConceptsCacheServiceFastAPI(ConceptProtocol):
    """FastAPI-compatible concepts cache service"""

    def __init__(self, app: FastAPI):
        self.app = app

    def get_is_abuse(self, concept: str) -> bool:
        return concept in self.app.state.taxonomy_cache["abuse"]

    def get_taxonomy_concept_label(self, taxonomy, concept_id: str) -> str:
        return self.app.state.taxonomy_cache["labels"][taxonomy].get(concept_id, None)

    @classmethod
    async def setup_cache(cls, tagstore_db, app: FastAPI):
        taxs = await tagstore_db.get_taxonomies(
            {Taxonomies.CONCEPT, Taxonomies.COUNTRY}
        )
        app.state.taxonomy_cache = {
            "labels": {
                Taxonomies.CONCEPT: {x.id: x.label for x in (taxs.concept or [])},
                Taxonomies.COUNTRY: {x.id: x.label for x in (taxs.country or [])},
            },
            "abuse": {x.id for x in (taxs.concept or []) if x.is_abuse},
        }

    @classmethod
    def setup_empty_cache(cls, app: FastAPI):
        app.state.taxonomy_cache = {
            "labels": {
                Taxonomies.CONCEPT: {},
                Taxonomies.COUNTRY: {},
            },
            "abuse": set(),
        }


async def setup_services(app: FastAPI):
    """Setup service container"""
    config = app.state.config

    if config.tag_access_logger and config.tag_access_logger.enabled:
        logger.info("Tag access logging is enabled.")
        from redis import asyncio as aioredis

        redis_url = config.tag_access_logger.redis_url or "redis://localhost"
        logger.info(f"Connecting to Redis at {redis_url} for tag access logging.")
        redis_client = await aioredis.from_url(redis_url)
        log_tag_access_prefix = config.tag_access_logger.prefix
    else:
        redis_client = None
        log_tag_access_prefix = None

    # Store redis_client on app.state for cleanup during shutdown
    app.state.redis_client = redis_client

    app.state.services = ServiceContainer(
        config=config,
        db=app.state.db,
        tagstore_db=app.state.tagstore_db,
        concepts_cache_service=ConceptsCacheServiceFastAPI(app),
        logger=logger,
        redis_client=redis_client,
        log_tag_access_prefix=log_tag_access_prefix,
    )


async def setup_plugins(app: FastAPI):
    """Setup plugins"""
    config = app.state.config
    app.state.plugins = []
    app.state.plugin_contexts = {}

    obfuscate_private_tags = any(
        1 for name in config.plugins if name.endswith("obfuscate_tags")
    )

    if obfuscate_private_tags:
        builtin_plugin = ObfuscateTags
        name = f"{builtin_plugin.__module__}"
        app.state.plugins.append(builtin_plugin)
        plugin_config = config.get_plugin_config(name)
        if plugin_config is None:
            # Backward compatibility: users often configure the builtin plugin
            # under the plugin entry path from config.plugins.
            for configured_name in config.plugins:
                if configured_name.endswith("obfuscate_tags"):
                    plugin_config = config.get_plugin_config(configured_name)
                    if plugin_config is not None:
                        break

        logger.warning(
            f"Tag obfuscation plugin enabled, using built-in version. "
            f"Skipping load of external plugin. Config: {plugin_config}"
        )
        app.state.plugin_contexts[name] = {"config": plugin_config}
        if hasattr(builtin_plugin, "setup"):
            setup_args = {
                "config": plugin_config,
                "context": app.state.plugin_contexts[name],
            }
            setup_gen = builtin_plugin.setup(setup_args)
            # If setup is an async generator, iterate it for startup
            if hasattr(setup_gen, "__anext__"):
                await setup_gen.__anext__()
                app.state.plugin_cleanup_generators = [setup_gen]

    for name in config.plugins:
        if name.endswith("obfuscate_tags"):
            continue

        subcl = get_subclass(importlib.import_module(name))
        app.state.plugins.append(subcl)
        app.state.plugin_contexts[name] = {}
        if hasattr(subcl, "setup"):
            plugin_config = config.get_plugin_config(name)
            setup_args = {
                "config": plugin_config,
                "context": app.state.plugin_contexts[name],
            }
            setup_gen = subcl.setup(setup_args)
            if hasattr(setup_gen, "__anext__"):
                await setup_gen.__anext__()
                app.state.plugin_cleanup_generators.append(setup_gen)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    await setup_database(app)
    await setup_services(app)
    await setup_plugins(app)

    yield

    # Shutdown
    # Close plugin cleanup generators
    for gen in getattr(app.state, "plugin_cleanup_generators", []):
        try:
            await gen.aclose()
        except Exception as e:
            logger.warning(f"Error closing plugin generator: {e}")

    await teardown_database(app)


def _get_request_context(request: Request) -> str:
    """Get user and URL context for error logging."""
    username = request.headers.get("X-Consumer-Username", "unknown")
    return f"URL: {request.url} | User: {username}"


def _register_exception_handlers(app: FastAPI):
    """Register common exception handlers on the app"""

    @app.exception_handler(NotFoundException)
    async def not_found_handler(request: Request, exc: NotFoundException):
        logger.warning(
            f"NotFoundException: {exc.get_user_msg()} | {_get_request_context(request)}"
        )
        return JSONResponse(
            status_code=404,
            content={"detail": exc.get_user_msg()},
        )

    @app.exception_handler(BadUserInputException)
    async def bad_input_handler(request: Request, exc: BadUserInputException):
        logger.warning(
            f"BadUserInputException: {exc.get_user_msg()} | {_get_request_context(request)}"
        )
        return JSONResponse(
            status_code=400,
            content={"detail": exc.get_user_msg()},
        )

    @app.exception_handler(PydanticValidationError)
    @app.exception_handler(PydanticCoreValidationError)
    async def pydantic_validation_handler(request: Request, exc: Exception):
        logger.warning(
            f"PydanticValidationError: {str(exc)} | {_get_request_context(request)}"
        )
        return JSONResponse(
            status_code=422,
            content={"detail": str(exc)},
        )

    @app.exception_handler(FeatureNotAvailableException)
    async def feature_not_available_handler(
        request: Request, exc: FeatureNotAvailableException
    ):
        logger.warning(
            f"FeatureNotAvailableException: {exc.get_user_msg()} | {_get_request_context(request)}"
        )
        return JSONResponse(
            status_code=400,
            content={"detail": exc.get_user_msg()},
        )

    @app.exception_handler(GsTimeoutException)
    async def timeout_handler(request: Request, exc: GsTimeoutException):
        logger.warning(f"GsTimeoutException | {_get_request_context(request)}")
        return JSONResponse(
            status_code=408,
            content={"detail": "Request timeout"},
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        logger.error(f"Unhandled exception | {_get_request_context(request)}\n{tb}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )


def _register_routers(app: FastAPI):
    """Register all API routers on the app."""
    app.include_router(general.router, tags=["general"])
    app.include_router(tags.router, tags=["tags"])
    app.include_router(
        addresses.router,
        prefix="/{currency}",
        tags=["addresses"],
    )
    app.include_router(blocks.router, prefix="/{currency}", tags=["blocks"])
    app.include_router(
        clusters.router,
        prefix="/{currency}",
        tags=["clusters"],
    )
    app.include_router(
        entities.router,
        prefix="/{currency}",
        tags=["entities"],
    )
    app.include_router(txs.router, prefix="/{currency}", tags=["txs"])
    app.include_router(rates.router, prefix="/{currency}", tags=["rates"])
    app.include_router(tokens.router, prefix="/{currency}", tags=["tokens"])
    app.include_router(bulk.router, prefix="/{currency}", tags=["bulk"])


def _get_api_dependencies(config: GSRestConfig) -> list:
    return [] if config.disable_auth else [Depends(get_api_key)]


def _promote_common_security_to_global(schema: dict[str, Any]) -> dict[str, Any]:
    """Promote repeated operation-level security to top-level OpenAPI security."""
    operation_security: list[Any] = []

    for path_item in schema.get("paths", {}).values():
        if not isinstance(path_item, dict):
            continue
        for operation in path_item.values():
            if not isinstance(operation, dict):
                continue
            if "security" in operation:
                operation_security.append(operation["security"])

    if not operation_security:
        return schema

    first_security = operation_security[0]
    if not all(sec == first_security for sec in operation_security):
        return schema

    schema["security"] = first_security

    for path_item in schema.get("paths", {}).values():
        if not isinstance(path_item, dict):
            continue
        for operation in path_item.values():
            if not isinstance(operation, dict):
                continue
            if operation.get("security") == first_security:
                operation.pop("security", None)

    return schema


def _setup_cors_middleware(app: FastAPI, config: GSRestConfig):
    """Setup CORS middleware on the app.

    When ALLOWED_ORIGINS contains "*", we use allow_origin_regex instead of
    allow_origins=["*"]. This makes the middleware echo back the requesting
    Origin header instead of sending literal "*", which allows credentials
    to work (browsers reject Access-Control-Allow-Origin: * with credentials).
    """
    origins = config.ALLOWED_ORIGINS
    if isinstance(origins, str):
        origins = [origins]

    # allow_origins=["*"] sends literal "*" which is incompatible with credentials.
    # Using allow_origin_regex=".*" echoes the Origin header, allowing credentials.
    # Check if "*" is anywhere in the list (not just exactly ["*"])
    if "*" in origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origin_regex=".*",
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
        )
    else:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
        )


def resolve_rest_config(
    config_file: str = None,
    config: Optional[GSRestConfig] = None,
    gslib_config: Optional[AppConfig] = None,
) -> GSRestConfig:
    """Resolve REST config with fallback chain.

    Priority:
    1. config object passed via create_app(config=...) (tests)
    2. Explicit config_file parameter
    3. CONFIG_FILE env var
    4. Default path (./instance/config.yaml)
    5. .graphsense.yaml 'web' key
    6. Pure env vars (GSRestConfig())
    """
    if config is not None:
        return config

    if config_file:
        raw_config = load_config(config_file)
        return GSRestConfig.from_dict(raw_config)

    config_file_from_env = os.environ.get("CONFIG_FILE")
    if config_file_from_env and os.path.exists(config_file_from_env):
        raw_config = load_config(config_file_from_env)
        return GSRestConfig.from_dict(raw_config)

    if os.path.exists(CONFIG_FILE):
        raw_config = load_config(CONFIG_FILE)
        return GSRestConfig.from_dict(raw_config)

    if gslib_config and gslib_config.underlying_file:
        raw_config = load_config(gslib_config.underlying_file)
        if "web" in raw_config:
            return GSRestConfig.from_dict(raw_config["web"])

    return GSRestConfig()


def create_app(
    config_file: str = None,
    validate_responses: bool = False,
    config: Optional[GSRestConfig] = None,
) -> FastAPI:
    """FastAPI application factory

    Args:
        config_file: Path to YAML config file
        validate_responses: Whether to validate responses (for testing)
        config: Pre-built GSRestConfig object (for testing, overrides config_file)
    """
    # Load gslib config (always needed for slack hooks)
    gslib_config = AppConfig()
    gslib_config.load_partial()

    config = resolve_rest_config(config_file, config, gslib_config)

    slack_exception_hook = config.get_slack_hooks_by_topic(
        "exceptions"
    ) or gslib_config.get_slack_hooks_by_topic("exceptions")
    slack_info_hook = config.get_slack_hooks_by_topic(
        "info"
    ) or gslib_config.get_slack_hooks_by_topic("info")
    default_environment = config.environment or gslib_config.default_environment
    config.slack_info_hook = slack_info_hook

    setup_logging(logger, slack_exception_hook, default_environment, config.logging)
    log_slack_exception_notification_status(slack_exception_hook, default_environment)

    app = FastAPI(
        title="GraphSense API",
        description=API_DESCRIPTION,
        version=__api_version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url="/openapi.json",
        dependencies=_get_api_dependencies(config),
    )

    app.state.config = config

    logger.info(f"ALLOWED_ORIGINS: {config.ALLOWED_ORIGINS}")
    _setup_cors_middleware(app, config)

    # Plugin middleware
    app.add_middleware(PluginMiddleware)

    # Empty params middleware (must be after PluginMiddleware to run first)
    app.add_middleware(EmptyQueryParamsMiddleware)

    # Advertise deprecation on responses from routes marked deprecated=True.
    app.add_middleware(DeprecationHeaderMiddleware)

    _register_exception_handlers(app)
    _register_routers(app)
    _setup_custom_openapi(app)
    _setup_custom_docs_ui(app)

    return app


def _setup_custom_openapi(app: FastAPI) -> None:
    """Set up custom OpenAPI schema generation with snake_case schema names.

    This ensures backward compatibility with the original Connexion-based API:
    - Uses snake_case schema names (e.g., 'address_tag' instead of 'AddressTag')
    - Adds named union schemas for tx, link, address_tx, tag types
    - Replaces inline anyOf with $ref to named union schemas

    The Python client generator depends on these schema names and union types.
    """

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )

        # Add servers for client generator compatibility
        openapi_schema["servers"] = [{"url": ""}]

        info = openapi_schema["info"]
        config = app.state.config

        # Add documentation metadata for compatibility and richer docs rendering
        info["contact"] = {
            "name": config.docs_contact_name,
            "email": config.docs_contact_email,
            "url": config.docs_contact_url,
        }

        openapi_schema["info"]["description"] = API_DESCRIPTION
        external_docs_url = config.docs_external_url
        if external_docs_url:
            openapi_schema["externalDocs"] = {
                "description": config.docs_external_label,
                "url": external_docs_url,
            }

        python_client_docs_url = config.docs_python_client_url
        if python_client_docs_url:
            openapi_schema["x-relatedDocs"] = [
                {
                    "label": config.docs_python_client_label,
                    "url": python_client_docs_url,
                }
            ]

        logo_url = _get_docs_logo_url(app)
        if logo_url:
            openapi_schema["info"]["x-logo"] = {
                "url": logo_url,
                "altText": app.title,
            }

        # Promote schema-level examples to parameter-level for Swagger UI
        openapi_schema = _promote_schema_examples_to_parameter_level(openapi_schema)

        # Normalize selected parameter examples for generated SDK snippets
        openapi_schema = _normalize_parameter_examples_for_generated_clients(
            openapi_schema
        )

        # Convert schema names to snake_case for backward compatibility
        openapi_schema = _convert_schema_names_to_snake_case(openapi_schema)

        # Promote repeated operation security requirements to global OpenAPI security
        openapi_schema = _promote_common_security_to_global(openapi_schema)

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi


def _get_docs_logo_url(app: FastAPI) -> Optional[str]:
    config = app.state.config
    custom_logo_url = config.docs_logo_url
    if custom_logo_url:
        return custom_logo_url
    if os.path.isfile(f"{DOCS_STATIC_DIR}/logo.png"):
        return DEFAULT_DOCS_LOGO_URL
    return None


def _get_docs_favicon_url(app: FastAPI) -> Optional[str]:
    config = app.state.config
    custom_favicon_url = config.docs_favicon_url
    if custom_favicon_url:
        return custom_favicon_url
    if os.path.isfile(f"{DOCS_STATIC_DIR}/favicon.ico"):
        return DEFAULT_DOCS_FAVICON_ICO_URL
    if os.path.isfile(f"{DOCS_STATIC_DIR}/favicon.png"):
        return DEFAULT_DOCS_FAVICON_PNG_URL
    return None


def _get_docs_links(app: FastAPI, page: str) -> list[tuple[str, str]]:
    config = app.state.config
    links: list[tuple[str, str]] = []

    if page == "swagger":
        crosslink_url = config.docs_swagger_crosslink_url
        crosslink_label = config.docs_swagger_crosslink_label
    else:
        crosslink_url = config.docs_redoc_crosslink_url
        crosslink_label = config.docs_redoc_crosslink_label

    if crosslink_url:
        links.append((crosslink_label, crosslink_url))

    python_client_url = config.docs_python_client_url
    if python_client_url:
        python_client_label = config.docs_python_client_label
        links.append((python_client_label, python_client_url))

    external_url = config.docs_external_url
    if external_url:
        external_label = config.docs_external_label
        links.append((external_label, external_url))

    return links


def _build_docs_links_block(links: list[tuple[str, str]], class_name: str) -> str:
    if not links:
        return ""
    anchors = "".join(
        f'<a class="gs-docs-link" href="{escape(url, quote=True)}">{escape(label)}</a>'
        for label, url in links
    )
    return f"""
<style>
  .{class_name} {{
    position: fixed;
    top: 12px;
    right: 12px;
    z-index: 1000;
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    justify-content: flex-end;
    max-width: 70vw;
  }}
  .{class_name} .gs-docs-link {{
    display: inline-block;
    padding: 6px 10px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 600;
    text-decoration: none;
    background: rgba(37, 50, 56, 0.9);
    color: #fff;
  }}
  .{class_name} .gs-docs-link:hover {{
    background: rgba(37, 50, 56, 1);
  }}
</style>
<div class="{class_name}">{anchors}</div>
"""


def _setup_custom_docs_ui(app: FastAPI) -> None:
    app.mount(
        DOCS_STATIC_URL,
        StaticFiles(directory=DOCS_STATIC_DIR, check_dir=False),
        name="docs-assets",
    )

    @app.get("/ui", include_in_schema=False)
    async def custom_swagger_ui() -> HTMLResponse:
        favicon_url = _get_docs_favicon_url(app)
        swagger_kwargs = {
            # Hide all OpenAPI vendor extensions (x-*) in Swagger UI.
            "swagger_ui_parameters": {
                "showExtensions": False,
                "showCommonExtensions": False,
            }
        }
        if favicon_url:
            swagger_kwargs["swagger_favicon_url"] = favicon_url
        response = get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=f"{app.title} - Swagger UI",
            **swagger_kwargs,
        )
        links_block = _build_docs_links_block(
            _get_docs_links(app, page="swagger"), class_name="gs-docs-links-swagger"
        )
        logo_url = _get_docs_logo_url(app)
        if not logo_url and not links_block:
            return response
        html = response.body.decode("utf-8")
        if logo_url:
            swagger_ui_logo_inject = f"""
<style>
  .swagger-ui .gs-docs-logo-block {{
    margin: 8px 0 16px 0;
    padding: 6px 0;
  }}
  .swagger-ui .gs-docs-logo {{
    display: block;
    height: 40px;
    width: auto;
  }}
</style>
<script>
  (function () {{
    function applyLogo() {{
      var infoContainer = document.querySelector(".swagger-ui div.information-container");
      if (!infoContainer) return;
      if (document.querySelector(".swagger-ui .gs-docs-logo-block")) return;

      var section = infoContainer.nextElementSibling;
      while (section && section.tagName !== "SECTION") {{
        section = section.nextElementSibling;
      }}

      var target = section || infoContainer;
      if (!target) return;

      var wrapper = document.createElement("div");
      wrapper.className = "gs-docs-logo-block";
      var logo = document.createElement("img");
      logo.src = "{logo_url}";
      logo.alt = "{app.title}";
      logo.className = "gs-docs-logo";
      wrapper.appendChild(logo);
      target.prepend(wrapper);
    }}
    var observer = new MutationObserver(applyLogo);
    observer.observe(document.documentElement, {{ childList: true, subtree: true }});
    window.addEventListener("load", applyLogo);
    applyLogo();
  }})();
</script>
"""
            html = html.replace("</head>", f"{swagger_ui_logo_inject}</head>")
        if links_block:
            html = html.replace("</body>", f"{links_block}</body>")
        return HTMLResponse(content=html, status_code=response.status_code)

    @app.get("/docs", include_in_schema=False)
    async def custom_redoc_ui() -> HTMLResponse:
        favicon_url = _get_docs_favicon_url(app)
        redoc_kwargs = {"redoc_favicon_url": favicon_url} if favicon_url else {}
        response = get_redoc_html(
            openapi_url=app.openapi_url,
            title=f"{app.title} - ReDoc",
            **redoc_kwargs,
        )
        links_block = _build_docs_links_block(
            _get_docs_links(app, page="redoc"), class_name="gs-docs-links-redoc"
        )
        logo_url = _get_docs_logo_url(app)
        if not logo_url and not links_block:
            return response
        html = response.body.decode("utf-8")
        if logo_url:
            redoc_logo_padding_css = """
<style>
  .redoc-wrap .menu-content img {
    padding: 6px;
  }
</style>
"""
            html = html.replace("</head>", f"{redoc_logo_padding_css}</head>")
        if links_block:
            html = html.replace("</body>", f"{links_block}</body>")
        return HTMLResponse(content=html, status_code=response.status_code)


def create_spec_app() -> FastAPI:
    """Create a minimal FastAPI app for OpenAPI spec generation (no DB/config needed)."""
    app = FastAPI(
        title="GraphSense API",
        description=API_DESCRIPTION,
        version=__api_version__,
        dependencies=[Depends(get_api_key)],
    )
    app.state.config = GSRestConfig.model_validate(
        {
            "disable_auth": False,
            "database": {"nodes": ["localhost"]},
            "gs-tagstore": {
                "url": "postgresql+asyncpg://user:password@localhost:5432/tagstore"
            },
        }
    )
    _register_routers(app)
    _setup_custom_openapi(app)
    return app


def create_app_from_dict(config_dict: dict) -> FastAPI:
    """Create FastAPI app from config dictionary (for testing)"""
    config = GSRestConfig.from_dict(config_dict)

    app = FastAPI(
        title="GraphSense API",
        version=__api_version__,
        lifespan=lifespan,
        dependencies=_get_api_dependencies(config),
    )

    app.state.config = config

    _setup_cors_middleware(app, config)
    app.add_middleware(PluginMiddleware)

    _register_exception_handlers(app)
    _register_routers(app)
    _setup_custom_openapi(app)

    return app
