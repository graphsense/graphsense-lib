import pytest

from graphsenselib.mcp import curation as curation_mod


def test_load_valid(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    assert "get_statistics" in c.include
    assert c.consolidated_tools[0].name == "lookup_address"
    assert c.replaced_op_ids() == {"get_address", "get_address_entity"}
    assert c.included_op_ids() == {"get_statistics", "get_block"}


def test_load_missing_file(tmp_path):
    with pytest.raises(curation_mod.CurationError):
        curation_mod.load(tmp_path / "nope.yaml")


def test_validate_against_app_ok(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    app_ops = {"get_statistics", "get_block", "get_address", "get_address_entity"}
    curation_mod.validate_against_app(c, app_ops)  # does not raise


def test_validate_against_app_missing_included(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    app_ops = {"get_block", "get_address", "get_address_entity"}  # no get_statistics
    with pytest.raises(curation_mod.CurationError) as exc:
        curation_mod.validate_against_app(c, app_ops)
    assert "get_statistics" in str(exc.value)


def test_validate_against_app_missing_replaced(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    app_ops = {"get_statistics", "get_block", "get_address"}  # no get_address_entity
    with pytest.raises(curation_mod.CurationError) as exc:
        curation_mod.validate_against_app(c, app_ops)
    assert "get_address_entity" in str(exc.value)


def test_validate_against_app_overlap(tmp_path):
    import yaml

    path = tmp_path / "tools.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "include": {"get_address": {"description": "x"}},
                "consolidated_tools": [
                    {
                        "name": "lookup_address",
                        "replaces": ["get_address"],
                        "module": "graphsenselib.mcp.tools.consolidated:register_lookup_address",
                    }
                ],
            }
        )
    )
    c = curation_mod.load(path)
    with pytest.raises(curation_mod.CurationError) as exc:
        curation_mod.validate_against_app(c, {"get_address"})
    assert "both in 'include'" in str(exc.value)


def test_bundled_curation_matches_real_app():
    """Source-of-truth smoke test: bundled curation must validate against the
    real spec app. Same gate `graphsense-cli mcp validate-curation` uses in CI.
    """
    from graphsenselib.mcp.config import GSMCPConfig
    from graphsenselib.mcp.server import validate_curation
    from graphsenselib.web.app import create_spec_app

    errors = validate_curation(create_spec_app(), GSMCPConfig())
    assert errors == [], f"bundled curation drift: {errors}"
