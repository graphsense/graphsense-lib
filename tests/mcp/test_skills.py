"""The bundled skills: served as MCP resources, packaged as a Claude Code
plugin, and kept in sync with the curated tool surface.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from fastmcp import Client

from graphsenselib.mcp import GSMCPConfig, build_mcp

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILLS_DIR = GSMCPConfig().bundled_skills_dir()
SKILL_FILES = sorted(SKILLS_DIR.glob("*/SKILL.md"))


def _frontmatter(path: Path) -> dict:
    _, fm, _ = path.read_text(encoding="utf-8").split("---", 2)
    return yaml.safe_load(fm)


async def _build(monkeypatch, **cfg):
    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", raising=False)
    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__API_KEY_ENV", raising=False)

    from graphsenselib.web.app import create_spec_app

    return build_mcp(create_spec_app(), GSMCPConfig(**cfg))


def test_bundled_skills_exist():
    assert [p.parent.name for p in SKILL_FILES] == [
        "identifier-integrity",
        "investigate-advisor",
        "investigate-autonomous",
        "investigate-strict",
        "investigation-reporting",
        "trace-funds",
    ]


async def test_skills_served_as_resources(monkeypatch):
    mcp, stack = await _build(monkeypatch)
    async with stack, Client(mcp) as c:
        uris = {str(r.uri) for r in await c.list_resources()}
        for p in SKILL_FILES:
            assert f"skill://{p.parent.name}/SKILL.md" in uris
            assert f"skill://{p.parent.name}/_manifest" in uris
        # The plugin manifest lives in the same directory but is no skill.
        assert not any(".claude-plugin" in u for u in uris)

        text = (await c.read_resource("skill://trace-funds/SKILL.md"))[0].text
        assert "TAGS INFORM, TRANSFERS PROVE" in text


async def test_skills_can_be_disabled(monkeypatch):
    mcp, stack = await _build(monkeypatch, skills_enabled=False)
    async with stack, Client(mcp) as c:
        uris = {str(r.uri) for r in await c.list_resources()}
    assert not any(u.startswith("skill://") for u in uris)


@pytest.mark.parametrize("skill_file", SKILL_FILES, ids=lambda p: p.parent.name)
async def test_skill_tools_are_exposed(monkeypatch, skill_file):
    """Every tool a skill declares must exist, and be mentioned in its body,
    so renaming or dropping a tool from the curation fails here."""
    metadata = _frontmatter(skill_file).get("metadata") or {}
    declared = metadata.get("graphsense-tools", "").split()
    body = skill_file.read_text(encoding="utf-8")

    mcp, stack = await _build(monkeypatch)
    async with stack, Client(mcp) as c:
        exposed = {t.name for t in await c.list_tools()}

    assert set(declared) - exposed == set()
    assert [t for t in declared if f"`{t}`" not in body] == []


@pytest.mark.parametrize("skill_file", SKILL_FILES, ids=lambda p: p.parent.name)
def test_referenced_skills_exist(skill_file):
    """The investigators load the shared skills by plugin-qualified name."""
    refs = set(re.findall(r"graphsense:([a-z0-9-]+)", skill_file.read_text()))
    assert refs - {p.parent.name for p in SKILL_FILES} == set()


@pytest.mark.parametrize("skill_file", SKILL_FILES, ids=lambda p: p.parent.name)
def test_skill_frontmatter(skill_file):
    fm = _frontmatter(skill_file)
    assert fm["name"] == skill_file.parent.name
    assert fm["description"]


def test_plugin_and_marketplace_agree():
    plugin = json.loads((SKILLS_DIR / ".claude-plugin" / "plugin.json").read_text())
    marketplace = json.loads(
        (REPO_ROOT / ".claude-plugin" / "marketplace.json").read_text()
    )
    (entry,) = marketplace["plugins"]
    assert entry["name"] == plugin["name"]
    assert (REPO_ROOT / entry["source"]).resolve() == SKILLS_DIR.resolve()
    assert plugin["skills"] == "./"
