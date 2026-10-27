"""Base configuration for API models."""

from typing import ClassVar, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

T = TypeVar("T", bound="APIModel")


def api_model_config(example: Optional[dict] = None) -> ConfigDict:
    """Create a ConfigDict with shared defaults and an optional Swagger example."""
    cfg = {"populate_by_name": True, "from_attributes": True}
    if example is not None:
        cfg["json_schema_extra"] = {"example": example}
    return ConfigDict(**cfg)


class APIModel(BaseModel):
    """Base class for all API models with shared configuration."""

    model_config = api_model_config()

    # Fields the OpenAPI schema should not list as required although the model
    # still requires them, e.g. a deprecated alias the server always sends but
    # clients must stop relying on before it goes.
    schema_optional: ClassVar[tuple[str, ...]] = ()

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        if cls.schema_optional:
            target = handler.resolve_ref_schema(json_schema)
            required = [
                f for f in target.get("required", []) if f not in cls.schema_optional
            ]
            if required:
                target["required"] = required
            else:
                target.pop("required", None)
        return json_schema

    @classmethod
    def from_dict(cls: type[T], dikt: dict) -> T:
        """Create model from dict (backward compatible with old OpenAPI models)."""
        return cls.model_validate(dikt)

    def to_dict(self, shallow: bool = False) -> dict:
        """Convert model to dict (backward compatible with old OpenAPI models).

        Args:
            shallow: If True, return raw values without recursively converting
                     nested models to dicts. This is used by bulk CSV flattening.
                     ALL fields are included (even None) to ensure consistent CSV columns.

        Returns:
            Dictionary representation of the model.
        """
        if shallow:
            # Return raw values for bulk flattening - include ALL fields for CSV columns
            return {key: value for key, value in self.__dict__.items()}

        # Normal case: recursively convert to dict, excluding None
        return self.model_dump(exclude_none=True)
