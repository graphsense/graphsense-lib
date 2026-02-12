import json

import click


@click.group()
def web_cli():
    pass


@web_cli.group("web")
def web():
    """Web API management tools."""
    pass


@web.command("openapi")
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Write to file instead of stdout.",
)
@click.option(
    "--spec-version",
    type=click.Choice(["3.1", "3.0"]),
    default="3.1",
    help="OpenAPI version (3.0 for older generators).",
)
def openapi_cmd(output, spec_version):
    """Export the OpenAPI specification as JSON."""
    from graphsenselib.web.app import create_spec_app

    app = create_spec_app()
    schema = app.openapi()

    if spec_version == "3.0":
        from graphsenselib.web.openapi_compat import convert_openapi_31_to_30

        schema = convert_openapi_31_to_30(schema)

    text = json.dumps(schema, indent=2)

    if output:
        with open(output, "w") as f:
            f.write(text)
            f.write("\n")
        click.echo(f"Written to {output}")
    else:
        click.echo(text)
