# ruff: noqa: T201
import json
import os
import sys
import tempfile
import time
from multiprocessing import Pool, cpu_count

import click
import pandas as pd
import yaml
from git import Repo
from tabulate import tabulate
from yaml.parser import ParserError, ScannerError

from graphsenselib.tagpack import get_version
from graphsenselib.tagpack.actorpack import Actor, ActorPack
from graphsenselib.tagpack.actorpack_schema import ActorPackSchema
from graphsenselib.tagpack.graphsense import GraphSense
from graphsenselib.tagpack.tagpack import (
    TagPack,
    TagPackFileError,
    collect_tagpack_files,
    get_repository,
    get_uri_for_tagpack,
)
from graphsenselib.tagpack.tagpack_schema import TagPackSchema, ValidationError
from graphsenselib.tagpack.tagstore import InsertTagpackWorker, TagStore
from graphsenselib.tagpack.utils import strip_empty
from graphsenselib.tagpack.constants import (
    CONFIG_FILE,
    DEFAULT_CONFIG,
    DEFAULT_SCHEMA,
)
from graphsenselib.tagstore.cli import tagstore
from graphsenselib.tagpack.taxonomy import _load_taxonomies, _load_taxonomy
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


def override_postgres_url(url):
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        logger.warning(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
    return url


def _load_config(cfile):
    if cfile is None or not os.path.isfile(cfile):
        return DEFAULT_CONFIG
    return yaml.safe_load(open(cfile, "r"))


def read_url_from_env():
    """
    Read environment variables from the OS, and build a postgresql connection
    URL. If the URL cannot be built, return None and an error message.
    """
    ev = dict(os.environ)
    try:
        url = f"postgresql://{ev['POSTGRES_USER']}:{ev['POSTGRES_PASSWORD']}"
        url += f"@{ev['POSTGRES_HOST']}:5432/{ev['POSTGRES_DB']}"
        msg = ""
    except KeyError:
        fields = ["USER", "PASSWORD", "HOST", "DB"]
        miss = {f"POSTGRES_{a}" for a in fields}
        miss -= set(ev.keys())
        msg = (
            "Unable to build postgresql URL: required environment variables "
            "(POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DB) not found: "
            + ", ".join(miss)
        )
        url = None
    return url, msg


def show_version():
    return f"GraphSense TagPack management tool {get_version()}"


@click.group()
def tagpacktool_cli():
    """tagpack management tool commands"""
    pass


@tagpacktool_cli.group("tagpack-tool")
@click.option(
    "--config",
    default=os.path.join(os.getcwd(), CONFIG_FILE),
    help="path to config.yaml",
)
@click.version_option(version=get_version(), prog_name="tagpack-tool")
@click.pass_context
def cli(ctx, config):
    """GraphSense TagPack management tool"""
    ctx.ensure_object(dict)
    ctx.obj["config"] = config


@cli.command()
@click.option("-v", "--verbose", is_flag=True, help="verbose configuration")
@click.pass_context
def config(ctx, verbose):
    """show repository config"""
    config_file = ctx.obj["config"]
    if os.path.exists(config_file):
        logger.info("Using Config File:", config_file)
    else:
        logger.info(
            f"No override config file found at {config_file}. Using default values."
        )
    if verbose:
        config_data = _load_config(config_file)
        logger.info("Show configured taxonomies")
        count = 0
        if "taxonomies" not in config_data:
            logger.error("No configured taxonomies")
        else:
            for key, value in config_data["taxonomies"].items():
                logger.info(value)
                count += 1
            click.secho(f"{count} configured taxonomies", fg="green")


@cli.command()
@click.option(
    "-r",
    "--repos",
    default=os.path.join(os.getcwd(), "tagpack-repos.config"),
    help="File with list of repos to sync to the database.",
)
@click.option(
    "--force",
    is_flag=True,
    help="By default, tagpack/actorpack insertion stops when an already inserted tagpack/actorpack exists in the database. Use this switch to force re-insertion.",
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option(
    "--run-cluster-mapping-with-env",
    help="Environment in graphsense-lib config to use for the mapping process. Only inserts non existing mappings.",
)
@click.option(
    "--rerun-cluster-mapping-with-env",
    help="Environment in graphsense-lib config to use for the mapping process. Reinserts all mappings.",
)
@click.option(
    "--n-workers",
    type=int,
    default=1,
    help="number of workers to use for the tagpack insert. Default is 1. Zero or negative values are used as offset of the machines cpu_count.",
)
@click.option(
    "--no-validation",
    is_flag=True,
    help="Do not validate tagpacks before insert. (better insert speed)",
)
@click.option(
    "--dont-update-quality-metrics",
    is_flag=True,
    help="Do update quality metrinc. (better insert speed)",
)
def sync(
    repos,
    force,
    url,
    run_cluster_mapping_with_env,
    rerun_cluster_mapping_with_env,
    n_workers,
    no_validation,
    dont_update_quality_metrics,
):
    """syncs the tagstore with a list of git repos."""
    url = override_postgres_url(url)

    if os.path.isfile(repos):
        with open(repos, "r") as f:
            repos_list = [x.strip() for x in f.readlines() if not x.startswith("#")]

        logger.info("Init db and add taxonomies ...")

        from graphsenselib.tagstore.cli import (
            tagstore as tagstore_tool,
            init as init_tagstore_tool,
        )

        click_ctx_tagstore = click.Context(tagstore_tool)

        click_ctx_actorpack = click.Context(actorpack)
        click_ctx_actorpack.ensure_object(dict)

        click_ctx_tagpack = click.Context(tagpack)
        click_ctx_tagpack.ensure_object(dict)

        click_ctx_tagpacktool_tagstore = click.Context(tagstore)
        click_ctx_tagpacktool_tagstore.ensure_object(dict)

        click_ctx_tagpacktool_quality = click.Context(quality)
        click_ctx_tagpacktool_quality.ensure_object(dict)

        click_ctx_tagstore.invoke(init_tagstore_tool, db_url=url)

        extra_option = "--force" if force else None
        extra_option = "--add-new" if extra_option is None else extra_option

        for repo_url in repos_list:
            with tempfile.TemporaryDirectory(suffix="tagstore_sync") as temp_dir_tt:
                logger.info(f"Syncing {repo_url}. Temp files in: {temp_dir_tt}")

                logger.info("Cloning...")
                repo_url, *branch_etc = repo_url.split(" ")
                repo = Repo.clone_from(repo_url, temp_dir_tt)
                if len(branch_etc) > 0:
                    branch = branch_etc[0]
                    logger.info(f"Using branch {branch}")
                    repo.git.checkout(branch)

                logger.info("Inserting actorpacks ...")

                click_ctx_actorpack.invoke(
                    insert_actorpack_cli, path=temp_dir_tt, url=url
                )

                logger.info("Inserting tagpacks ...")
                public = len(branch_etc) > 1 and branch_etc[1].strip() == "public"

                tag_type_default = (
                    branch_etc[2].strip() if len(branch_etc) > 2 else None
                )

                if public:
                    logger.info("Caution: This repo is imported as public.")

                args = {
                    "path": temp_dir_tt,
                    "url": url,
                    "public": public,
                    "force": force,
                    "n_workers": n_workers,
                    "no_validation": no_validation,
                    "add_new": False,
                    "update": not force,
                    "tag_type_default": tag_type_default,
                }

                if tag_type_default is None:
                    args.pop("tag_type_default")

                click_ctx_tagpack.invoke(insert_tagpack_cli, **args)

        logger.info("Removing duplicates ...")
        click_ctx_tagstore.invoke(remove_duplicates, url=url)

        logger.info("Refreshing db views ...")
        click_ctx_tagstore.invoke(refresh_views, url=url)

        if not dont_update_quality_metrics:
            logger.info("Calc Quality metrics ...")
            calc_quality_measures(url, DEFAULT_SCHEMA)
            # click_ctx_tagpacktool_quality.invoke(calculate_quality, url=url)

        if run_cluster_mapping_with_env or rerun_cluster_mapping_with_env:
            logger.info("Import cluster mappings ...")

            click_ctx_tagpacktool_tagstore.invoke(
                insert_cluster_mappings,
                url=url,
                use_gs_lib_config_env=(
                    run_cluster_mapping_with_env or rerun_cluster_mapping_with_env
                ),
                update=rerun_cluster_mapping_with_env is not None,
            )

            logger.info("Refreshing db views ...")
            click_ctx_tagstore.invoke(refresh_views, url=url)

        click.secho("Your tagstore is now up-to-date again.", fg="green")

    else:
        logger.error(f"Repos to sync file {repos} does not exist.")


def validate_tagpack(config, path, no_address_validation):
    t0 = time.time()
    logger.info("TagPack validation starts")
    logger.info(f"Path: {path}")

    taxonomies = _load_taxonomies(config)
    taxonomy_keys = taxonomies.keys()
    logger.info(f"Loaded taxonomies: {taxonomy_keys}")

    schema = TagPackSchema()
    logger.info(f"Loaded schema: {schema.definition}")

    tagpack_files = collect_tagpack_files(path)
    n_tagpacks = len([f for fs in tagpack_files.values() for f in fs])
    logger.info(f"Collected {n_tagpacks} TagPack files")

    no_passed = 0
    try:
        for headerfile_dir, files in tagpack_files.items():
            for tagpack_file in files:
                tagpack = TagPack.load_from_file(
                    "", tagpack_file, schema, taxonomies, headerfile_dir
                )

                logger.info(f"Validating {tagpack_file}")

                tagpack.validate()
                # verify valid blocknetwork addresses using internal checksum
                if not no_address_validation:
                    tagpack.verify_addresses()

                click.secho("PASSED", fg="green")

                no_passed += 1
    except (ValidationError, TagPackFileError) as e:
        logger.error(f"FAILED: {e}")

    failed = no_passed < n_tagpacks

    duration = round(time.time() - t0, 2)
    if failed:
        logger.error(f"{no_passed}/{n_tagpacks} TagPacks passed in {duration}s")
    else:
        click.secho(
            f"{no_passed}/{n_tagpacks} TagPacks passed in {duration}s", fg="green"
        )

    if failed:
        sys.exit(1)


def list_tags(url, schema, unique, category, network, csv):
    t0 = time.time()
    if not csv:
        logger.info("List tags starts")

    tagstore = TagStore(url, schema)

    try:
        uniq, cat, net = unique, category, network
        qm = tagstore.list_tags(unique=uniq, category=cat, network=net)
        if not csv:
            logger.info(f"{len(qm)} Tags found")
        else:
            logger.info("network,tp_title,tag_label")
        for row in qm:
            logger.info(("," if csv else ", ").join(map(str, row)))

        duration = round(time.time() - t0, 2)
        if not csv:
            click.secho(f"Done in {duration}s", fg="green")
    except Exception as e:
        logger.error(f"Operation failed: {e}")


def _suggest_actors(url, schema, label, max_results):
    logger.info(f"Searching suitable actors for {label} in TagStore")
    tagstore = TagStore(url, schema)
    candidates = tagstore.find_actors_for(
        label, max_results, use_simple_similarity=False, threshold=0.1
    )
    print(f"Found {len(candidates)} candidates")
    df = pd.DataFrame(candidates)
    print(
        tabulate(
            df,
            headers=df.columns,
            tablefmt="psql",
            maxcolwidths=[None, None, None, None, 60],
        )
    )


def add_actors_to_tagpack(url, schema, path, max_results, categories, inplace):
    logger.info("Starting interactive tagpack actor enrichment process.")

    tagstore = TagStore(url, schema)
    tagpack_files = collect_tagpack_files(path)

    schema_obj = TagPackSchema()
    user_choice_cache = {}

    for headerfile_dir, files in tagpack_files.items():
        for tagpack_file in files:
            tagpack = TagPack.load_from_file(
                "", tagpack_file, schema_obj, None, headerfile_dir
            )
            logger.info(f"Loading {tagpack_file}: ")

            def find_actor_candidates(search_term):
                res = tagstore.find_actors_for(
                    search_term,
                    max_results,
                    use_simple_similarity=False,
                    threshold=0.1,
                )

                def get_label(actor_row):
                    a = Actor.from_contents(actor_row, None)
                    return f"{actor_row['label']} ({', '.join(a.uris)})"

                return [(x["id"], get_label(x)) for x in res]

            category_filter = strip_empty(categories.split(","))
            updated = tagpack.add_actors(
                find_actor_candidates,
                only_categories=category_filter if len(category_filter) > 0 else None,
                user_choice_cache=user_choice_cache,
            )

            if updated:
                updated_file = (
                    tagpack_file.replace(".yaml", "_with_actors.yaml")
                    if not inplace
                    else tagpack_file
                )
                click.secho(f"Writing updated Tagpack {updated_file}", fg="green")
                with open(updated_file, "w") as outfile:
                    tagpack.contents["tags"] = tagpack.contents.pop(
                        "tags"
                    )  # re-insert tags
                    tagpack.update_lastmod()
                    yaml.dump(
                        tagpack.contents, outfile, sort_keys=False
                    )  # write in order of insertion
            else:
                click.secho("No actors added, moving on.", fg="green")


def insert_tagpack(
    url,
    schema,
    path,
    batch_size,
    public,
    force,
    add_new,
    no_strict_check,
    no_git,
    n_workers,
    no_validation,
    tag_type_default,
    config,
    update_flag,
) -> Tuple[int, int]:
    t0 = time.time()
    logger.info("TagPack insert starts")
    logger.info(f"Path: {path}")

    assert not (update_flag and add_new), "Can't use update and add_new together."

    if no_git:
        base_url = path
        logger.info("No repository detection done.")
    else:
        base_url = get_repository(path)
        logger.info(f"Detected repository root in {base_url}")

    tagstore = TagStore(url, schema)

    schema_obj = TagPackSchema()
    logger.info(f"Loaded TagPack schema definition: {schema_obj.definition}")

    config_data = _load_config(config)
    taxonomies = _load_taxonomies(config_data)
    taxonomy_keys = taxonomies.keys()
    logger.info(f"Loaded taxonomies: {taxonomy_keys}")

    tagpack_files = collect_tagpack_files(path)

    # resolve backlinks to remote repository and relative paths
    scheck, nogit = not no_strict_check, no_git
    prepared_packs = [
        (m, h, n[0], n[1], n[2], n[3], False)
        for m, h, n in [
            (a, h, get_uri_for_tagpack(base_url, a, scheck, nogit))
            for h, fs in tagpack_files.items()
            for a in fs
        ]
    ]

    prefix = None  # config.get("prefix", None)

    if update_flag:  # update existing tagpacks if modified, skip unmodified ones
        logger.info("Checking which files are new or modified in the tagstore:")
        prepared_packs = [
            (
                t,
                h,
                u,
                r,
                default_prefix,
                lastmod,
                tagstore.tp_exists(prefix if prefix else default_prefix, r),
            )
            for (t, h, u, r, default_prefix, lastmod, _) in prepared_packs
            if tagstore.tp_needs_update(
                prefix if prefix else default_prefix, r, lastmod
            )
        ]

    if add_new:  # don't re-insert existing tagpacks
        logger.info("Checking which files are new to the tagstore:")
        prepared_packs = [
            (t, h, u, r, default_prefix, lastmod, False)
            for (t, h, u, r, default_prefix, lastmod, _) in prepared_packs
            if not tagstore.tp_exists(prefix if prefix else default_prefix, r)
        ]

    n_ppacks = len(prepared_packs)

    logger.info(f"Collected {n_ppacks} TagPack files")

    packs = enumerate(sorted(prepared_packs), start=1)

    n_processes = n_workers if n_workers > 0 else cpu_count() + n_workers

    if n_processes < 1:
        logger.error(f"Can't use {n_processes} adjust your n_workers setting.")
        sys.exit(100)

    if n_processes > 1:
        logger.info(f"Running parallel insert on {n_processes} workers.")

    worker = InsertTagpackWorker(
        url,
        schema,
        schema_obj,
        taxonomies,
        public,
        force,
        updateMode=update_flag,
        validate_tagpack=not no_validation,
        tag_type_default=tag_type_default,
        no_git=no_git,
    )

    if n_processes != 1:
        with Pool(processes=n_processes) as pool:
            results = list(pool.imap_unordered(worker, packs, chunksize=10))
    else:
        # process data in the main process, makes debugging easier
        results = [worker(p) for p in packs]

    if results is not None and len(results) > 0:
        no_passed, no_tags = [sum(x) for x in zip(*results)]
    else:
        no_passed, no_tags = (0, 0)

    status = "fail" if no_passed < n_ppacks else "success"

    duration = round(time.time() - t0, 2)
    msg = "Processed {}/{} TagPacks with {} Tags in {}s. "
    if status == "fail":
        logger.error(msg.format(no_passed, n_ppacks, no_tags, duration))
    else:
        click.secho(msg.format(no_passed, n_ppacks, no_tags, duration), fg="green")
    msg = "Don't forget to run 'graphsense-cli tagstore refresh-views' soon to keep the database"
    msg += " consistent!"
    print(msg)

    return (no_passed, n_ppacks)


@cli.group("tagpack")
def tagpack():
    """commands regarding tags and tagpacks"""
    pass


@tagpack.command("validate")
@click.argument("path", default=os.getcwd())
@click.option(
    "--no-address-validation",
    is_flag=True,
    help="Disables checksum validation of addresses",
)
@click.pass_context
def validate_tagpack_cli(ctx, path, no_address_validation):
    """validate TagPacks"""
    config = _load_config(ctx.obj.get("config"))
    validate_tagpack(config, path, no_address_validation)


@tagpack.command("list")
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--unique", is_flag=True, help="List Tags removing duplicates")
@click.option("--category", default="", help="List Tags of a specific category")
@click.option(
    "--network", default="", help="List Tags of a specific crypto-currency network"
)
@click.option("--csv", is_flag=True, help="Show csv output.")
def list_tagpack_cli(schema, url, unique, category, network, csv):
    """list Tags"""
    url = override_postgres_url(url)
    list_tags(url, schema, unique, category, network, csv)


@tagpack.command("insert")
@click.argument("path", default=os.getcwd())
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option(
    "-b", "--batch-size", type=int, default=1000, help="batch size for insert"
)
@click.option(
    "--public",
    is_flag=True,
    help="By default, tagpacks are declared private in the database. Use this switch to declare them public.",
)
@click.option(
    "--force",
    is_flag=True,
    help="By default, tagpack insertion stops when an already inserted tagpack exists in the database. Use this switch to force re-insertion.",
)
@click.option(
    "--add-new",
    is_flag=True,
    help="By default, tagpack insertion stops when an already inserted tagpack exists in the database. Use this switch to insert new tagpacks while skipping over existing ones.",
)
@click.option(
    "--no-strict-check",
    is_flag=True,
    help="Disables check for local modifications in git repository",
)
@click.option("--no-git", is_flag=True, help="Disables check for local git repository")
@click.option(
    "--n-workers",
    type=int,
    default=1,
    help="number of workers to use for the tagpack insert. Default is 1. Zero or negative values are used as offset of the machines cpu_count.",
)
@click.option(
    "--no-validation",
    is_flag=True,
    help="Do not validate tagpacks before insert. (better insert speed)",
)
@click.option(
    "--tag-type-default",
    type=str,
    default="actor",
    help="Default value for tag-type if missing in the tagpack. Default is legacy value actor.",
)
@click.option(
    "--update",
    is_flag=True,
    help="By default, tagpack insertion stops when an already inserted tagpack exists in the database. Use this switch to update existing tagpacks if modified, but skip unmodified ones.",
)
@click.pass_context
def insert_tagpack_cli(
    ctx,
    path,
    schema,
    url,
    batch_size,
    public,
    force,
    add_new,
    no_strict_check,
    no_git,
    n_workers,
    no_validation,
    tag_type_default,
    update,
):
    """insert TagPacks"""
    url = override_postgres_url(url)

    insert_tagpack(
        url,
        schema,
        path,
        batch_size,
        public,
        force,
        add_new,
        no_strict_check,
        no_git,
        n_workers,
        no_validation,
        tag_type_default,
        ctx.obj.get("config"),
        update,
    )


@tagpack.command()
@click.argument("label")
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--max", type=int, default=5, help="Limits the number of results")
def suggest_actors(schema, url, label, max):
    """suggest an actor based on input"""
    url = override_postgres_url(url)
    _suggest_actors(url, schema, label, max)


@tagpack.command()
@click.argument("path", default=os.getcwd())
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--max", type=int, default=5, help="Limits the number of results")
@click.option(
    "--categories",
    default="",
    help="Only edit tags of a certain category (multiple possible with semi-colon)",
)
@click.option(
    "--inplace",
    is_flag=True,
    help="If set the source tagpack file is overwritten, otherwise a new file is generated called [original_file]_with_actors.yaml.",
)
def add_actors(path, schema, url, max, categories, inplace):
    """interactively add actors to tagpack"""
    url = override_postgres_url(url)
    add_actors_to_tagpack(url, schema, path, max, categories, inplace)


def validate_actorpack(config, path):
    t0 = time.time()
    logger.info("ActorPack validation starts")
    logger.info(f"Path: {path}")

    taxonomies = _load_taxonomies(config)
    taxonomy_keys = taxonomies.keys()
    logger.info(f"Loaded taxonomies: {taxonomy_keys}")

    schema = ActorPackSchema()
    logger.info(f"Loaded schema: {schema.definition}")

    actorpack_files = collect_tagpack_files(path, search_actorpacks=True)
    n_actorpacks = len([f for fs in actorpack_files.values() for f in fs])
    logger.info(f"Collected {n_actorpacks} ActorPack files")

    no_passed = 0
    try:
        for headerfile_dir, files in actorpack_files.items():
            for actorpack_file in files:
                actorpack = ActorPack.load_from_file(
                    "", actorpack_file, schema, taxonomies, headerfile_dir
                )

                logger.info(f"{actorpack_file}:\n", end="")

                actorpack.validate()
                click.secho("PASSED", fg="green")

                no_passed += 1
    except (ValidationError, TagPackFileError, ParserError, ScannerError) as e:
        logger.error(f"FAILED: {e}")

    failed = no_passed < n_actorpacks

    status = "fail" if failed else "success"

    duration = round(time.time() - t0, 2)
    msg = f"{no_passed}/{n_actorpacks} ActorPacks passed in {duration}s"
    if status == "fail":
        logger.error(msg)
    else:
        click.secho(msg, fg="green")

    if failed:
        sys.exit(1)


def insert_actorpacks(
    url, schema, path, batch_size, force, add_new, no_strict_check, no_git, config
):
    t0 = time.time()
    logger.info("ActorPack insert starts")
    logger.info(f"Path: {path}")

    if no_git:
        base_url = path
        logger.info("No repository detection done.")
    else:
        base_url = get_repository(path)
        logger.info(f"Detected repository root in {base_url}")

    tagstore = TagStore(url, schema)

    schema_obj = ActorPackSchema()
    logger.info(f"Loaded ActorPack schema definition: {schema_obj.definition}")

    config_data = _load_config(config)
    taxonomies = _load_taxonomies(config_data)
    taxonomy_keys = taxonomies.keys()
    logger.info(f"Loaded taxonomies: {taxonomy_keys}")

    actorpack_files = collect_tagpack_files(path, search_actorpacks=True)

    # resolve backlinks to remote repository and relative paths
    # For the URI we use the same logic for ActorPacks than for TagPacks
    scheck, nogit = not no_strict_check, no_git
    prepared_packs = [
        (m, h, n[0], n[1], n[2])
        for m, h, n in [
            (a, h, get_uri_for_tagpack(base_url, a, scheck, nogit))
            for h, fs in actorpack_files.items()
            for a in fs
        ]
    ]

    prefix = None  # config.get("prefix", None)
    if add_new:  # don't re-insert existing tagpacks
        logger.info("Checking which ActorPacks are new to the tagstore:")
        prepared_packs = [
            (t, h, u, r, default_prefix)
            for (t, h, u, r, default_prefix) in prepared_packs
            if not tagstore.actorpack_exists(prefix if prefix else default_prefix, r)
        ]

    n_ppacks = len(prepared_packs)
    logger.info(f"Collected {n_ppacks} ActorPack files")

    no_passed = 0
    no_actors = 0

    for i, pack in enumerate(sorted(prepared_packs), start=1):
        actorpack_file, headerfile_dir, uri, relpath, default_prefix = pack

        actorpack = ActorPack.load_from_file(
            uri, actorpack_file, schema_obj, taxonomies, headerfile_dir
        )

        logger.info(f"{i} {actorpack_file}: ", end="")
        try:
            tagstore.insert_actorpack(
                actorpack, force, prefix if prefix else default_prefix, relpath
            )
            click.secho(f"PROCESSED {len(actorpack.actors)} Actors", fg="green")
            no_passed += 1
            no_actors += len(actorpack.actors)
        except Exception as e:
            logger.error(f"FAILED: {e}")

    status = "fail" if no_passed < n_ppacks else "success"

    duration = round(time.time() - t0, 2)
    msg = "Processed {}/{} ActorPacks with {} Actors in {}s."
    if status == "fail":
        logger.error(msg.format(no_passed, n_ppacks, no_actors, duration))
    else:
        click.secho(msg.format(no_passed, n_ppacks, no_actors, duration), fg="green")


def list_actors(url, schema, category, csv):
    t0 = time.time()
    if not csv:
        logger.info("List actors starts")

    tagstore = TagStore(url, schema)

    try:
        qm = tagstore.list_actors(category=category)
        if not csv:
            print(f"{len(qm)} Actors found")
        else:
            print("actorpack,actor_id,actor_label,concept_label")

        for row in qm:
            print(("," if csv else ", ").join(map(str, row)))

        duration = round(time.time() - t0, 2)
        if not csv:
            click.secho(f"Done in {duration}s", fg="green")
    except Exception as e:
        logger.error(f"Operation failed: {e}")


def list_address_actors(url, schema, network, csv):
    t0 = time.time()
    if not csv:
        logger.info("List addresses with actor tags starts")

    tagstore = TagStore(url, schema)

    try:
        qm = tagstore.list_address_actors(network=network)
        if not csv:
            print(f"{len(qm)} addresses found")
        else:
            print("tag_id,tag_label,tag_address,tag_category,actor_label")

        for row in qm:
            print((", " if not csv else ",").join(map(str, row)))

        duration = round(time.time() - t0, 2)
        if not csv:
            click.secho(f"Done in {duration}s", fg="green")
    except Exception as e:
        logger.error(f"Operation failed: {e}")


# Move this to the top level of the module
class ClusterMappingArgs:
    def __init__(
        self,
        url,
        schema,
        db_nodes,
        cassandra_username,
        cassandra_password,
        ks_file,
        use_gs_lib_config_env,
        update,
    ):
        self.url = url
        self.schema = schema
        self.db_nodes = db_nodes
        self.cassandra_username = cassandra_username
        self.cassandra_password = cassandra_password
        self.ks_file = ks_file
        self.use_gs_lib_config_env = use_gs_lib_config_env
        self.update = update


@cli.group("actorpack")
def actorpack():
    """commands regarding actor information"""
    pass


@actorpack.command("validate")
@click.argument("path", default=os.getcwd())
@click.pass_context
def validate_actorpack_cli(ctx, path):  # noqa: F811
    """validate ActorPacks"""
    config = _load_config(ctx.obj.get("config"))
    validate_actorpack(config, path)


@actorpack.command("insert")
@click.argument("path", default=os.getcwd())
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for actorpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option(
    "-b", "--batch-size", type=int, default=1000, help="batch size for insert"
)
@click.option(
    "--force",
    is_flag=True,
    help="By default, actorpack insertion stops when an already inserted actorpack exists in the database. Use this switch to force re-insertion.",
)
@click.option(
    "--add-new",
    is_flag=True,
    help="By default, actorpack insertion stops when an already inserted actorpack exists in the database. Use this switch to insert new actorpacks while skipping over existing ones.",
)
@click.option(
    "--no-strict-check",
    is_flag=True,
    help="Disables check for local modifications in git repository",
)
@click.option("--no-git", is_flag=True, help="Disables check for local git repository")
@click.pass_context
def insert_actorpack_cli(
    ctx, path, schema, url, batch_size, force, add_new, no_strict_check, no_git
):  # noqa: F811
    """insert ActorPacks"""
    url = override_postgres_url(url)
    insert_actorpacks(
        url,
        schema,
        path,
        batch_size,
        force,
        add_new,
        no_strict_check,
        no_git,
        ctx.obj.get("config"),
    )


@actorpack.command("list")
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--category", default="", help="List Actors of a specific category")
@click.option("--csv", is_flag=True, help="Show csv output.")
def list_actorpack_cli(schema, url, category, csv):  # noqa: F811
    """list Actors"""
    url = override_postgres_url(url)
    list_actors(url, schema, category, csv)


@actorpack.command()
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option(
    "--network", default="", help="List addresses of a specific crypto-currency network"
)
@click.option("--csv", is_flag=True, help="Show csv output.")
def list_address_actor(schema, url, network, csv):
    """list addresses-actors"""
    url = override_postgres_url(url)
    list_address_actors(url, schema, network, csv)


def list_taxonomies(config):
    logger.info("Show configured taxonomies")
    count = 0
    if "taxonomies" not in config:
        logger.error("No configured taxonomies")
    else:
        for key, value in config["taxonomies"].items():
            logger.info(value)
            count += 1
        click.secho(f"{count} configured taxonomies", fg="green")


def show_taxonomy_concepts(config, taxonomy, tree, verbose):
    from anytree import RenderTree

    if "taxonomies" not in config:
        logger.error("No taxonomies configured")
        return

    print(f"Showing concepts of taxonomy {taxonomy}")
    uri = config["taxonomies"][taxonomy]
    print(f"URI: {uri}\n")
    taxonomy_obj = _load_taxonomy(config, taxonomy)
    if tree:
        for pre, fill, node in RenderTree(taxonomy_obj.get_concept_tree()):
            print("%s%s" % (pre, node.name))
    else:
        if verbose:
            headers = ["Id", "Label", "Level", "Uri", "Description"]
            table = [
                [c.id, c.label, c.level, c.uri, c.description]
                for c in taxonomy_obj.concepts
            ]
        elif taxonomy == "confidence":
            headers = ["Level", "Label"]
            table = [[c.level, c.label] for c in taxonomy_obj.concepts]
        else:
            headers = ["Id", "Label"]
            table = [[c.id, c.label] for c in taxonomy_obj.concepts]

        print(tabulate(table, headers=headers))
        print(f"{len(taxonomy_obj.concepts)} taxonomy concepts")


def insert_taxonomy():
    logger.error(
        "tagpack-tool taxonomy insert was"
        " retired in favor of tagstore init, please use this command"
    )
    sys.exit(1)


@cli.group("taxonomy")
@click.pass_context
def taxonomy(ctx):
    """taxonomy commands"""
    # Default behavior when no subcommand is provided
    if ctx.invoked_subcommand is None:
        config = _load_config(ctx.obj.get("config"))
        list_taxonomies(config)


@taxonomy.command("list")
@click.pass_context
def list_taxonomies_cli(ctx):  # noqa: F811
    """list taxonomy concepts"""
    config = _load_config(ctx.obj.get("config"))
    list_taxonomies(config)


@taxonomy.command()
@click.argument(
    "taxonomy_key",
    type=click.Choice(["abuse", "entity", "confidence", "country", "concept"]),
)
@click.option("-v", "--verbose", is_flag=True, help="verbose concepts")
@click.option("--tree", is_flag=True, help="Show as tree")
@click.pass_context
def show(ctx, taxonomy_key, verbose, tree):
    """show taxonomy concepts"""
    config = _load_config(ctx.obj.get("config"))
    show_taxonomy_concepts(config, taxonomy_key, tree, verbose)


@taxonomy.command("insert")
@click.argument(
    "taxonomy_key",
    type=click.Choice(["abuse", "entity", "confidence", "country", "concept"]),
    required=False,
)
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for taxonomy tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
def insert_taxonomy_cli(taxonomy_key, schema, url):  # noqa: F811
    """insert taxonomy into GraphSense"""
    insert_taxonomy()


def _split_into_chunks(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def insert_cluster_mapping_wp(network, ks_mapping, args, batch):
    tagstore = TagStore(args.url, args.schema)
    gs = GraphSense(
        args.db_nodes,
        ks_mapping,
        username=args.cassandra_username,
        password=args.cassandra_password,
    )
    if gs.keyspace_for_network_exists(network):
        clusters = gs.get_address_clusters(batch, network)
        clusters["network"] = network
        tagstore.insert_cluster_mappings(clusters)
    else:
        clusters = []
        logger.error(
            "At least one of the configured keyspaces"
            f" for network {network} does not exist."
        )
    return (network, len(clusters))


def load_ks_mapping(args):
    if args.use_gs_lib_config_env:
        gs_config_file = os.path.expanduser("~/.graphsense.yaml")
        if os.path.exists(gs_config_file):
            with open(gs_config_file) as f:
                yml = yaml.safe_load(f)
                if args.use_gs_lib_config_env in yml["environments"]:
                    env = yml["environments"][args.use_gs_lib_config_env]
                    args.db_nodes = env["cassandra_nodes"]
                    args.cassandra_password = env.get(
                        "readonly_password", None
                    ) or env.get("password", None)
                    args.cassandra_username = env.get(
                        "readonly_username", None
                    ) or env.get("username", None)
                    ret = {
                        k.upper(): {
                            "raw": v["raw_keyspace_name"],
                            "transformed": v["transformed_keyspace_name"],
                        }
                        for k, v in env["keyspaces"].items()
                    }
                    return ret
                else:
                    logger.error(
                        f"Environment {args.use_gs_lib_config_env} "
                        "not found in gs-config"
                    )
                    sys.exit(1)

        else:
            logger.error("Graphsense config not found at ~/.graphsense.yaml")
            sys.exit(1)
    else:
        if args.ks_file and os.path.exists(args.ks_file):
            return json.load(open(args.ks_file))
        else:
            logger.error(f"Keyspace config file not found at {args.ks_file}")
            sys.exit(1)


def init_db():
    logger.error(
        "tagpack-tool init was retired"
        " in favor of tagstore init, please use this command"
    )
    sys.exit(1)


def insert_cluster_mapping(
    url,
    schema,
    db_nodes,
    cassandra_username,
    cassandra_password,
    ks_file,
    use_gs_lib_config_env,
    update,
    batch_size=5_000,
):
    # Use the module-level class instead
    args = ClusterMappingArgs(
        url,
        schema,
        db_nodes,
        cassandra_username,
        cassandra_password,
        ks_file,
        use_gs_lib_config_env,
        update,
    )

    t0 = time.time()
    tagstore = TagStore(url, schema)
    df = pd.DataFrame(tagstore.get_addresses(update), columns=["address", "network"])
    ks_mapping = load_ks_mapping(args)
    logger.info("Importing with mapping config: ", ks_mapping)
    networks = ks_mapping.keys()
    gs = GraphSense(
        args.db_nodes,
        ks_mapping,
        username=args.cassandra_username,
        password=args.cassandra_password,
    )

    workpackages = []
    for network, data in df.groupby("network"):
        if gs.contains_keyspace_mapping(network):
            for batch in _split_into_chunks(data, batch_size):
                workpackages.append((network, ks_mapping, args, batch))

    nr_workers = int(cpu_count() / 2)
    logger.info(
        f"Processing {len(workpackages)} batches for "
        f"{len(networks)} networks on {nr_workers} workers."
    )

    with Pool(processes=nr_workers, maxtasksperchild=1) as pool:
        processed_workpackages = pool.starmap(insert_cluster_mapping_wp, workpackages)

    processed_networks = {network for network, _ in processed_workpackages}

    for pc in processed_networks:
        mappings_count = sum(
            [items for network, items in processed_workpackages if network == pc]
        )
        click.secho(
            f"INSERTED/UPDATED {mappings_count} {pc} cluster mappings", fg="green"
        )

    tagstore.finish_mappings_update(networks)
    duration = round(time.time() - t0, 2)
    logger.info(
        f"Inserted {'missing' if not update else 'all'} cluster mappings "
        f"for {processed_networks} in {duration}s"
    )


def update_db(url, schema):
    tagstore = TagStore(url, schema)
    tagstore.refresh_db()
    logger.info("All relevant views have been updated.")


def _remove_duplicates(url, schema):
    tagstore = TagStore(url, schema)
    rows_deleted = tagstore.remove_duplicates()
    msg = f"{rows_deleted} duplicate tags have been deleted from the database."
    logger.info(msg)


def show_tagstore_composition(url, schema, csv, by_network):
    tagstore = TagStore(url, schema)
    headers = (
        ["creator", "group", "network", "labels_count", "tags_count"]
        if by_network
        else ["creator", "group", "labels_count", "tags_count"]
    )
    df = pd.DataFrame(
        tagstore.get_tagstore_composition(by_network=by_network), columns=headers
    )

    if csv:
        print(df.to_csv(header=True, sep=",", index=True))
    else:
        with pd.option_context(
            "display.max_rows", None, "display.max_columns", None
        ):  # more options can be specified also
            print(tabulate(df, headers=headers, tablefmt="psql"))


def show_tagstore_source_repos(url, schema, csv):
    tagstore = TagStore(url, schema)

    res = tagstore.tagstore_source_repos()
    df = pd.DataFrame(res)
    if csv:
        print(df.to_csv(header=True, sep=",", index=True))
    else:
        print(
            tabulate(
                df,
                headers=df.columns,
                tablefmt="psql",
                maxcolwidths=[None, None, 10, 50],
            )
        )


@cli.group("tagstore")
def tagstore():  # noqa: F811
    """database housekeeping commands"""
    pass


@tagstore.command()
@click.option(
    "--schema",
    default=DEFAULT_SCHEMA,
    help="PostgreSQL schema for GraphSense cluster mapping table",
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
def init(schema, url):
    """init the database"""
    init_db()


@tagstore.command()
@click.option(
    "-d",
    "--db-nodes",
    multiple=True,
    default=["localhost"],
    help='Cassandra node(s); default "localhost"',
)
@click.option("--cassandra-username", default=None, help="Cassandra Username")
@click.option("--cassandra-password", default=None, help="Cassandra password")
@click.option(
    "-f",
    "--ks-file",
    help="JSON file with Cassandra keyspaces that contain GraphSense cluster mappings",
)
@click.option(
    "--use-gs-lib-config-env",
    help="Load ks-mapping from global graphsense-lib config. Overrides --ks_file and --db_nodes",
)
@click.option(
    "--schema",
    default=DEFAULT_SCHEMA,
    help="PostgreSQL schema for GraphSense cluster mapping table",
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--update", is_flag=True, help="update all cluster mappings")
def insert_cluster_mappings(
    db_nodes,
    cassandra_username,
    cassandra_password,
    ks_file,
    use_gs_lib_config_env,
    schema,
    url,
    update,
):
    """insert cluster mappings"""
    url = override_postgres_url(url)
    insert_cluster_mapping(
        url,
        schema,
        list(db_nodes),
        cassandra_username,
        cassandra_password,
        ks_file,
        use_gs_lib_config_env,
        update,
    )


@tagstore.command()
@click.option(
    "--schema",
    default=DEFAULT_SCHEMA,
    help="PostgreSQL schema for GraphSense cluster mapping table",
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
def refresh_views(schema, url):
    """update views"""
    url = override_postgres_url(url)
    update_db(url, schema)


@tagstore.command()
@click.option(
    "--schema",
    default=DEFAULT_SCHEMA,
    help="PostgreSQL schema for GraphSense cluster mapping table",
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
def remove_duplicates(schema, url):
    """remove duplicate tags"""
    url = override_postgres_url(url)
    _remove_duplicates(url, schema)


@tagstore.command()
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--csv", is_flag=True, help="Show csv output.")
@click.option(
    "--by-network", is_flag=True, help="Include currency/network in statistic."
)
def show_composition(schema, url, csv, by_network):
    """Shows the tag composition grouped by creator and category."""
    url = override_postgres_url(url)
    show_tagstore_composition(url, schema, csv, by_network)


@tagstore.command()
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--csv", is_flag=True, help="Show csv output.")
def show_source_repos(schema, url, csv):
    """Shows which repos sources are stored in the database."""
    url = override_postgres_url(url)
    show_tagstore_source_repos(url, schema, csv)


def print_quality_measures(qm):
    if qm:
        print("Tag and Actor metrics:")
        tc = qm["tag_count"]
        tca = qm["tag_count_with_actors"]
        print(f"\t{'#Tags:':<35} {tc:10}")
        if tc > 0:
            print(f"\t{' with actors:':<35} {tca:10} ({(100 * tca) / tc:6.2f}%)")

        au = qm["nr_actors_used"]
        auj = qm["nr_actors_used_with_jurisdictions"]
        print(f"\n\t{'#Actors used:':<35} {au:10}")
        if au > 0:
            print(f"\t{' with jurisdictions:':<35} {auj:10} ({(100 * auj) / au:6.2f}%)")

        au_ex = qm["nr_actors_used_exchange"]
        auj_ex = qm["nr_actors_used_with_jurisdictions_exchange"]
        print(f"\n\t{'#Exchange-Actors used:':<35} {au_ex:10}")
        if au_ex > 0:
            print(
                f"\t{' with jurisdictions:':<35} "
                f"{auj_ex:10} ({(100 * auj_ex) / au_ex:6.2f}%)"
            )

        print("Tag Quality Statistics:")
        print(f"\t{'Quality COUNT:':<35} {qm['count']:10}")
        print(f"\t{'Quality AVG:':<35}    {qm['avg']:7.2f}")
        print(f"\t{'Quality STDDEV:':<35}    {qm['stddev']:7.2f}")
    else:
        print("\tNone")


def show_quality_measures(url, schema, network):
    logger.info("Show quality measures")
    tagstore = TagStore(url, schema)

    try:
        qm = tagstore.get_quality_measures(network)
        c = network if network else "Global"
        print(f"{c} quality measures:")
        print_quality_measures(qm)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error("Operation failed")


def calc_quality_measures(url, schema):
    t0 = time.time()
    logger.info("Calculate quality measures starts")

    tagstore = TagStore(url, schema)

    try:
        qm = tagstore.calculate_quality_measures()
        print("Global quality measures:")
        print_quality_measures(qm)

        duration = round(time.time() - t0, 2)
        click.secho(f"Done in {duration}s", fg="green")
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error("Operation failed")


def low_quality_addresses(url, schema, threshold, network, category, csv, cluster):
    if not csv:
        print("Addresses with low quality")
    tagstore = TagStore(url, schema)

    try:
        th, curr, cat = threshold, network, category
        la = tagstore.low_quality_address_labels(th, curr, cat)
        if la:
            if not csv:
                c = network if network else "all"
                print(f"List of {c} addresses and labels ({len(la)}):")
            else:
                print("network,address,labels")

            intersections = []
            for (network_key, address), labels in la.items():
                if csv:
                    labels_str = "|".join(labels)
                    print(f"{network_key},{address},{labels_str}")
                else:
                    print(f"\t{network_key}\t{address}\t{labels}")

                if not cluster:
                    continue

                # Produce clusters of addresses based on tag intersections
                seen = set()
                for i, (e, n) in enumerate(intersections):
                    seen = e.intersection(labels)
                    if seen:
                        e.update(labels)
                        n += 1
                        intersections[i] = (e, n)
                        break
                if not seen:
                    intersections.append((set(labels), 1))

            if cluster:
                print("\nSets of tags appearing in several addresses:")
                s_int = sorted(intersections, key=lambda x: x[1], reverse=True)
                for k, v in s_int:
                    if v > 1:
                        print(f"\t{v}: {', '.join(k)}")
        else:
            if not csv:
                print("\tNone")

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error("Operation failed")


def list_low_quality_actors(url, schema, category, max_results, not_used, csv):
    tagstore = TagStore(url, schema)

    res = tagstore.get_actors_with_jurisdictions(
        category=category, max_results=max_results, include_not_used=not_used
    )
    df = pd.DataFrame(res)
    if csv:
        print(df.to_csv(header=True, sep=",", index=True))
    else:
        print("Actors without Jurisdictions")
        print(
            tabulate(
                df,
                headers=df.columns,
                tablefmt="psql",
                maxcolwidths=[None, None, 10, 10, 60, 10],
            )
        )


def list_top_labels_without_actor(url, schema, category, max_results, csv):
    tagstore = TagStore(url, schema)

    res = tagstore.top_labels_without_actor(category=category, max_results=max_results)
    df = pd.DataFrame(res)
    if csv:
        print(df.to_csv(header=True, sep=",", index=True))
    else:
        print("Top labels without actor")
        print(
            tabulate(
                df,
                headers=df.columns,
                tablefmt="psql",
                maxcolwidths=[None, None, 10, 50],
            )
        )


def list_addresses_with_actor_collisions_impl(url, schema, csv):
    tagstore = TagStore(url, schema)

    res = tagstore.addresses_with_actor_collisions()
    df = pd.DataFrame(res)
    if csv:
        print(df.to_csv(header=True, sep=",", index=True))
    else:
        print("Addresses with actor collisions")
        print(
            tabulate(
                df,
                headers=df.columns,
                tablefmt="psql",
                maxcolwidths=[None, None, 10, 50],
            )
        )


@cli.group("quality")
@click.option(
    "--schema",
    default=DEFAULT_SCHEMA,
    help="PostgreSQL schema for quality measures tables",
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option(
    "--network",
    default="",
    help="Show the avg quality measure for a specific crypto-currency network",
)
@click.pass_context
def quality(ctx, schema, url, network):
    """calculate tags quality measures"""
    ctx.ensure_object(dict)
    url = override_postgres_url(url)

    ctx.obj["url"] = url
    ctx.obj["schema"] = schema
    ctx.obj["network"] = network

    # Default behavior when no subcommand is provided
    if ctx.invoked_subcommand is None:
        show_quality_measures(url, schema, network)


@quality.command("calculate")
@click.pass_context
def calculate_quality(ctx):
    """calculate quality measures for all tags in the DB"""
    calc_quality_measures(ctx.obj["url"], ctx.obj["schema"])


@quality.command()
@click.pass_context
def show(ctx):  # noqa: F811
    """show average quality measures"""
    show_quality_measures(ctx.obj["url"], ctx.obj["schema"], ctx.obj["network"])


@quality.command()
@click.option("--category", default="", help="List addresses of a specific category")
@click.option(
    "--network",
    default="",
    help="Show low quality addresses of a specific crypto-currency network",
)
@click.option(
    "--threshold",
    type=float,
    default=0.25,
    help="List addresses having a quality lower than this threshold",
)
@click.option(
    "-c",
    "--cluster",
    is_flag=True,
    help="Cluster addresses having intersections of similar tags",
)
@click.option("--csv", is_flag=True, help="Show csv output.")
@click.pass_context
def list_addresses_with_low_quality(ctx, category, network, threshold, cluster, csv):
    """list low quality addresses"""
    low_quality_addresses(
        ctx.obj["url"], ctx.obj["schema"], threshold, network, category, csv, cluster
    )


@quality.command()
@click.option("--category", default="", help="List actors of a specific category")
@click.option("--max", type=int, default=5, help="Limits the number of results")
@click.option(
    "--not-used", is_flag=True, help="Include actors that are not used in tags."
)
@click.option("--csv", is_flag=True, help="Show csv output.")
@click.pass_context
def list_actors_without_jur(ctx, category, max, not_used, csv):
    """actors without jurisdictions."""
    list_low_quality_actors(
        ctx.obj["url"], ctx.obj["schema"], category, max, not_used, csv
    )


@quality.command()
@click.option("--category", default="", help="List actors of a specific category")
@click.option("--max", type=int, default=5, help="Limits the number of results")
@click.option("--csv", is_flag=True, help="Show csv output.")
@click.pass_context
def list_labels_without_actor(ctx, category, max, csv):
    """List the top labels used in tags without actors."""
    list_top_labels_without_actor(ctx.obj["url"], ctx.obj["schema"], category, max, csv)


@quality.command()
@click.option("--csv", is_flag=True, help="Show csv output.")
@click.pass_context
def list_addresses_with_actor_collisions(ctx, csv):
    """List actors with address collisions."""
    list_addresses_with_actor_collisions_impl(ctx.obj["url"], ctx.obj["schema"], csv)


def main():
    """Main entry point for the tagpacktool_cli."""
    def_url, url_msg = read_url_from_env()

    if len(sys.argv) == 1:
        ctx = click.Context(cli)
        click.echo(cli.get_help(ctx))
        sys.exit(1)

    try:
        cli()
    except click.ClickException as e:
        if hasattr(e, "message") and "No postgresql URL" in str(e.message):
            logger.warning(
                "Missing required PostgreSQL environment variables for URL construction."
            )
        e.show()
        sys.exit(1)


if __name__ == "__main__":
    main()
