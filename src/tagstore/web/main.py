import os
from contextlib import asynccontextmanager
from typing import List, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from ..db.database import get_db_engine_async
from .dependencies import _gs_tagstore_settings
from .routers.admin import router as admin_router
from .routers.base import router as base_router


class SinglePageApplication(StaticFiles):
    """Acts similar to the bripkens/connect-history-api-fallback
    NPM package.

    Based of: https://stackoverflow.com/questions/63069190/how-to-capture-arbitrary-paths-at-one-route-in-fastapi
    """

    def __init__(self, packages: List[str], index="index.html") -> None:
        self.index = index

        # set html=True to resolve the index even when no
        # the base path is passed in
        super().__init__(packages=packages, html=True, check_dir=True)

    def lookup_path(self, path: str) -> Tuple[str, os.stat_result]:
        """Returns the index file when no match is found.

        Args:
            path (str): Resource path.

        Returns:
            [tuple[str, os.stat_result]]: Always retuens a full path and stat result.
        """
        x = super().lookup_path(path)

        full_path, stat_result = x
        # if a file cannot be found
        if stat_result is None:
            return super().lookup_path(self.index)

        return (full_path, stat_result)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global background_executor
    settings = _gs_tagstore_settings()
    app.state.db_engine = get_db_engine_async(settings.db_url)
    yield


app = FastAPI(
    title="Web API for the GraphSense Tagstore",
    lifespan=lifespan,
    contact={"email": "contact@ikna.io"},
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:1234"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(base_router, prefix="/api")
app.include_router(admin_router, prefix="/api/admin", include_in_schema=True)

app.mount("/", app=SinglePageApplication(packages=["tagstore.web"]), name="SPA")
