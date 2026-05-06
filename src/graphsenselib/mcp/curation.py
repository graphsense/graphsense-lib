from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field


class CurationError(Exception):
    """Raised when curation YAML is malformed or drifts from the FastAPI app."""


class IncludedTool(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: Optional[str] = Field(
        default=None,
        description="LLM-facing description override. If None, the FastAPI docstring is kept.",
    )
    tags: list[str] = Field(default_factory=list)


class ConsolidatedTool(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    replaces: list[str] = Field(default_factory=list)
    module: str = Field(
        description="Dotted <module>:<callable> that registers the tool on the FastMCP instance",
    )


class ExternalTool(BaseModel):
    model_config = ConfigDict(extra="forbid")

    module: str
    enabled: bool = True


class Defaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tag_prefix: str = "gs_"


class CurationFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = 1
    defaults: Defaults = Field(default_factory=Defaults)
    include: dict[str, IncludedTool] = Field(default_factory=dict)
    consolidated_tools: list[ConsolidatedTool] = Field(default_factory=list)
    external_tools: dict[str, ExternalTool] = Field(default_factory=dict)

    def replaced_op_ids(self) -> set[str]:
        result: set[str] = set()
        for tool in self.consolidated_tools:
            result.update(tool.replaces)
        return result

    def included_op_ids(self) -> set[str]:
        return set(self.include.keys())


def load(path: Path) -> CurationFile:
    if not path.exists():
        raise CurationError(f"Curation file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    try:
        return CurationFile.model_validate(raw)
    except Exception as exc:
        raise CurationError(f"Invalid curation file {path}: {exc}") from exc


def validate_against_app(curation: CurationFile, app_operation_ids: set[str]) -> None:
    """Fail fast if the curation references operation_ids that don't exist on the app,
    or if an op_id is listed both as an include and as a consolidated replacement.
    """
    included = curation.included_op_ids()
    replaced = curation.replaced_op_ids()

    missing_included = included - app_operation_ids
    missing_replaced = replaced - app_operation_ids
    overlap = included & replaced

    errors: list[str] = []
    if missing_included:
        errors.append(
            "Curation 'include' references unknown operation_ids: "
            + ", ".join(sorted(missing_included))
        )
    if missing_replaced:
        errors.append(
            "Curation 'consolidated_tools.replaces' references unknown operation_ids: "
            + ", ".join(sorted(missing_replaced))
        )
    if overlap:
        errors.append(
            "These operation_ids are both in 'include' and in a consolidated "
            "'replaces' list (consolidation must supersede passthrough): "
            + ", ".join(sorted(overlap))
        )
    if errors:
        raise CurationError("\n".join(errors))


def collect_operation_ids(app) -> set[str]:
    """Enumerate operation_ids on a FastAPI app."""
    ops: set[str] = set()
    for path, methods in app.openapi().get("paths", {}).items():
        for method, detail in methods.items():
            if method in ("get", "post", "put", "patch", "delete"):
                op = detail.get("operationId")
                if op:
                    ops.add(op)
    return ops
