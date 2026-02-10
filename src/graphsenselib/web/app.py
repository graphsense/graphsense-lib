import importlib
import json
import logging
import logging.handlers
import os
import re
import traceback
from contextlib import asynccontextmanager
from typing import Any, Optional

import yaml
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from graphsenselib.config import AppConfig
from graphsenselib.web.version import __api_version__
from graphsenselib.db.asynchronous.services.tags_service import ConceptProtocol
from graphsenselib.errors import (
    BadUserInputException,
    FeatureNotAvailableException,
    GsTimeoutException,
    NotFoundException,
)
from graphsenselib.tagstore.db import TagstoreDbAsync, Taxonomies
from graphsenselib.tagstore.db.database import get_db_engine_async
from graphsenselib.utils.slack import SlackLogHandler

from graphsenselib.web.builtin.plugins.obfuscate_tags.obfuscate_tags import (
    ObfuscateTags,
)
from graphsenselib.web.config import GSRestConfig, LoggingConfig
from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.middleware.empty_params import EmptyQueryParamsMiddleware
from graphsenselib.web.middleware.plugins import PluginMiddleware
from graphsenselib.web.plugins import get_subclass
from graphsenselib.web.routes import (
    addresses,
    blocks,
    bulk,
    entities,
    general,
    rates,
    tags,
    tokens,
    txs,
)
from graphsenselib.web.security import get_api_key

CONFIG_FILE = "./instance/config.yaml"
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
    max_conn = ts_conf.pool_size
    max_pool_time = ts_conf.pool_timeout
    mo = ts_conf.max_overflow
    recycle = ts_conf.pool_recycle
    enable_prepared_statements_cache = ts_conf.enable_prepared_statements_cache

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

    app.state.tagstore_engine = engine

    # Setup taxonomy cache
    await ConceptsCacheServiceFastAPI.setup_cache(engine, app)

    logger.info("Database setup done")


async def teardown_database(app: FastAPI):
    """Cleanup database connections"""
    logger.info("Begin app teardown")
    driver = app.state.config.database.driver.lower()
    app.state.db.close()
    logger.info(f"Closed {driver} connection.")
    logger.info(app.state.tagstore_engine.pool.status())
    await app.state.tagstore_engine.dispose()
    logger.info(app.state.tagstore_engine.pool.status())
    logger.info("Closed Tagstore connection.")

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
    async def setup_cache(cls, db_engine, app: FastAPI):
        tagstore_db = TagstoreDbAsync(db_engine)
        taxs = await tagstore_db.get_taxonomies(
            {Taxonomies.CONCEPT, Taxonomies.COUNTRY}
        )
        app.state.taxonomy_cache = {
            "labels": {
                Taxonomies.CONCEPT: {x.id: x.label for x in taxs.concept},
                Taxonomies.COUNTRY: {x.id: x.label for x in taxs.country},
            },
            "abuse": {x.id for x in taxs.concept if x.is_abuse},
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
        tagstore_engine=app.state.tagstore_engine,
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
        logger.warning(
            "Tag obfuscation plugin enabled, using built-in version. "
            "Skipping load of external plugin."
        )
        builtin_plugin = ObfuscateTags
        name = f"{builtin_plugin.__module__}"
        app.state.plugins.append(builtin_plugin)
        plugin_config = config.get_plugin_config(name)
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
    """Register all API routers on the app.

    All routers require api_key authentication unless disable_auth is set.
    """
    config = app.state.config
    api_key_dep = [] if config.disable_auth else [Depends(get_api_key)]
    app.include_router(general.router, tags=["general"], dependencies=api_key_dep)
    app.include_router(tags.router, tags=["tags"], dependencies=api_key_dep)
    app.include_router(
        addresses.router,
        prefix="/{currency}",
        tags=["addresses"],
        dependencies=api_key_dep,
    )
    app.include_router(
        blocks.router, prefix="/{currency}", tags=["blocks"], dependencies=api_key_dep
    )
    app.include_router(
        entities.router,
        prefix="/{currency}",
        tags=["entities"],
        dependencies=api_key_dep,
    )
    app.include_router(
        txs.router, prefix="/{currency}", tags=["txs"], dependencies=api_key_dep
    )
    app.include_router(
        rates.router, prefix="/{currency}", tags=["rates"], dependencies=api_key_dep
    )
    app.include_router(
        tokens.router, prefix="/{currency}", tags=["tokens"], dependencies=api_key_dep
    )
    app.include_router(
        bulk.router, prefix="/{currency}", tags=["bulk"], dependencies=api_key_dep
    )


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
    if config is None:
        if not config_file:
            config_file = CONFIG_FILE
        raw_config = load_config(config_file)
        config = GSRestConfig.from_dict(raw_config)

    # Load graphsense-lib config for slack hooks
    gslib_config = AppConfig()
    gslib_config.load()

    if gslib_config is not None:
        slack_exception_hook = gslib_config.get_slack_hooks_by_topic("exceptions")
        slack_info_hook = gslib_config.get_slack_hooks_by_topic("info")
        default_environment = config.environment or gslib_config.default_environment
    else:
        slack_exception_hook = None
        slack_info_hook = None
        default_environment = config.environment

    config.slack_info_hook = slack_info_hook

    setup_logging(logger, slack_exception_hook, default_environment, config.logging)

    app = FastAPI(
        title="GraphSense API",
        description="GraphSense API provides programmatic access to various cryptocurrency analytics features.",
        version=__api_version__,
        lifespan=lifespan,
        docs_url="/ui",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.state.config = config

    logger.info(f"ALLOWED_ORIGINS: {config.ALLOWED_ORIGINS}")
    _setup_cors_middleware(app, config)

    # Plugin middleware
    app.add_middleware(PluginMiddleware)

    # Empty params middleware (must be after PluginMiddleware to run first)
    app.add_middleware(EmptyQueryParamsMiddleware)

    _register_exception_handlers(app)
    _register_routers(app)
    _setup_custom_openapi(app)

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

        # Add contact info for backward compatibility
        openapi_schema["info"]["contact"] = {
            "email": "contact@ikna.io",
            "name": "Iknaio Cryptoasset Analytics GmbH",
        }
        openapi_schema["info"]["description"] = (
            "GraphSense API provides programmatic access to various ledgers' "
            "addresses, entities, blocks, transactions and tags for automated "
            "and highly efficient forensics tasks."
        )

        # Promote schema-level examples to parameter-level for Swagger UI
        openapi_schema = _promote_schema_examples_to_parameter_level(openapi_schema)

        # Convert schema names to snake_case for backward compatibility
        openapi_schema = _convert_schema_names_to_snake_case(openapi_schema)

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi


def create_app_from_dict(config_dict: dict) -> FastAPI:
    """Create FastAPI app from config dictionary (for testing)"""
    config = GSRestConfig.from_dict(config_dict)

    app = FastAPI(
        title="GraphSense API",
        version=__api_version__,
        lifespan=lifespan,
    )

    app.state.config = config

    _setup_cors_middleware(app, config)
    app.add_middleware(PluginMiddleware)

    _register_exception_handlers(app)
    _register_routers(app)
    _setup_custom_openapi(app)

    return app
