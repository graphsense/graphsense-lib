def test_unavailable_optional_cli_reports_import_error():
    from click.testing import CliRunner

    from graphsenselib.cli.main import _unavailable_cli

    stub = _unavailable_cli(
        ["tagpack-tool", "tagstore"], ImportError("No module named 'x'"), "tagpacks"
    )
    res = CliRunner().invoke(stub, ["tagpack-tool", "tagpack", "validate", "--foo"])
    assert res.exit_code != 0
    assert "'tagpack-tool' is not available: No module named 'x'" in res.output
    assert "graphsense-lib[tagpacks]" in res.output
