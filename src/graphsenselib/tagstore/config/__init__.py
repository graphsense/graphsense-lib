from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

# TODO(deprecation): remove with _legacy.py
from graphsenselib.config._legacy import _emit_class_deprecation


class TagstoreSettings(BaseSettings):
    def __init__(self, **kwargs: Any) -> None:
        # TODO(deprecation): remove with _legacy.py
        _emit_class_deprecation(
            "TagstoreSettings", "graphsenselib.config.Settings.tagstore"
        )
        super().__init__(**kwargs)

    db_url: str = "postgresql://graphsense:test@localhost:5432/tagstore"

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="gs_tagstore_", extra="ignore"
    )
