"""Base configuration for API models."""

from typing import TypeVar

from pydantic import BaseModel, ConfigDict

T = TypeVar("T", bound="APIModel")


class APIModel(BaseModel):
    """Base class for all API models with shared configuration."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
    )

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
