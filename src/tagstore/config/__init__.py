from pydantic_settings import BaseSettings, SettingsConfigDict


class TagstoreSettings(BaseSettings):
    db_url: str = "postgresql://graphsense:test@localhost:5432/tagstore"

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="gs_tagstore_", extra="ignore"
    )
