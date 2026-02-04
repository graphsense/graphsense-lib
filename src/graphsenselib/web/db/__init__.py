import importlib

from graphsenselib.tagstore.db.database import get_db_engine_async

from graphsenselib.web.dependencies import ConceptsCacheService


async def get_connection(app):
    config = app["config"].database
    driver = config.driver.lower()
    app.logger.info(f"Opening {driver} connection ...")
    mod = importlib.import_module("graphsenselib.db.asynchronous." + driver)
    cls = getattr(mod, driver.capitalize())

    app["db"] = cls(config, app.logger)

    ts_conf = app["config"].tagstore
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
        pool_pre_ping=True,  # resolves error on shutdown https://github.com/MagicStack/asyncpg/issues/309
        # echo=True,
    )

    # fetch taxonomy lookup
    await ConceptsCacheService.setup_cache(engine, app)

    app["gs-tagstore"] = engine

    app.logger.info("Setup done")

    yield

    app.logger.info("Begin app teardown")
    try:
        app["db"].close()
        app.logger.info(f"Closed {driver} connection.")
        app.logger.info(engine.pool.status())
        await engine.dispose()
        app.logger.info(engine.pool.status())
        app.logger.info("Closed Tagstore connection.")
    except Exception as x:
        app.logger.error(x)
        app.logger.error(x)
