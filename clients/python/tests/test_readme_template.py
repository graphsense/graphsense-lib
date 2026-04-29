"""Guard rail for the openapi-generator README template.

`templates/README.mustache` overrides the generator's default Python README.
If a future upstream refresh accidentally drops the hand-written
CLI/ext links section, this test fails loudly before a regen wipes the
section out of the rendered README.md.
"""

from __future__ import annotations

from pathlib import Path

TEMPLATE = Path(__file__).parent.parent / "templates" / "README.mustache"


def test_template_exists():
    assert TEMPLATE.exists(), (
        f"{TEMPLATE} is missing — the generator would fall back to its "
        "built-in template and drop the hand-written section"
    )


def test_template_contains_cli_ext_links_section():
    content = TEMPLATE.read_text()
    assert "## Using the high-level wrapper or CLI" in content
    assert "README_EXT.md" in content
    assert "README_CLI.md" in content


def test_template_preserves_upstream_structure():
    """Minimal smoke check — the template still looks like the upstream
    Mustache (title, description interpolation, version metadata, and the
    `common_README` partial include for the per-endpoint tables).
    Prevents shipping a template accidentally missing upstream content.
    """
    content = TEMPLATE.read_text()
    required_tokens = (
        "{{{projectName}}}",
        "appDescriptionWithNewLines",
        "{{appVersion}}",
        "{{packageVersion}}",
        "{{> common_README }}",
    )
    for token in required_tokens:
        assert token in content, f"template missing required token: {token!r}"
