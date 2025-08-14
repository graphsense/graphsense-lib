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
from colorama import init
from git import Repo
from tabulate import tabulate
from yaml.parser import ParserError, ScannerError

from graphsenselib.tagpack import get_version
from graphsenselib.tagpack.actorpack import Actor, ActorPack
from graphsenselib.tagpack.actorpack_schema import ActorPackSchema
from graphsenselib.tagpack.cmd_utils import (
    print_fail,
    print_info,
    print_line,
    print_success,
    print_warn,
)
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
from click.testing import CliRunner
from graphsenselib.tagstore.cli import tagstore
from graphsenselib.tagpack.taxonomy import _load_taxonomies, _load_taxonomy

init()


def _load_config(cfile):
    if not os.path.isfile(cfile):
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
        msg = "Unable to build postgresql URL from environment variables: "
        msg += ", ".join(miss) + " not found."
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
        print("Using Config File:", config_file)
    else:
        print_info(
            f"No override config file found at {config_file}. Using default values."
        )
    if verbose:
        config_data = _load_config(config_file)
        print_line("Show configured taxonomies")
        count = 0
        if "taxonomies" not in config_data:
            print_line("No configured taxonomies", "fail")
        else:
            for key, value in config_data["taxonomies"].items():
                print_info(value)
                count += 1
            print_line(f"{count} configured taxonomies", "success")


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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url

    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)

    if os.path.isfile(repos):
        with open(repos, "r") as f:
            repos_list = [x.strip() for x in f.readlines() if not x.startswith("#")]

        print_line("Init db and add taxonomies ...")

        runner = CliRunner()
        runner.invoke(tagstore, ["init", "--db-url", url], catch_exceptions=False)

        extra_option = "--force" if force else None
        extra_option = "--add-new" if extra_option is None else extra_option

        for repo_url in repos_list:
            with tempfile.TemporaryDirectory(suffix="tagstore_sync") as temp_dir_tt:
                print(f"Syncing {repo_url}. Temp files in: {temp_dir_tt}")

                print_info("Cloning...")
                repo_url, *branch_etc = repo_url.split(" ")
                repo = Repo.clone_from(repo_url, temp_dir_tt)
                if len(branch_etc) > 0:
                    branch = branch_etc[0]
                    print_info(f"Using branch {branch}")
                    repo.git.checkout(branch)

                print("Inserting actorpacks ...")

                runner.invoke(
                    cli,
                    ["actorpack", "insert", temp_dir_tt, "-u", url],
                    catch_exceptions=False,
                )

                print("Inserting tagpacks ...")
                public = len(branch_etc) > 1 and branch_etc[1].strip() == "public"

                tag_type_default = (
                    branch_etc[2].strip() if len(branch_etc) > 2 else None
                )

                if public:
                    print("Caution: This repo is imported as public.")

                args = strip_empty(
                    [
                        "tagpack",
                        "insert",
                        extra_option,
                        "--public" if public else None,
                        temp_dir_tt,
                        "-u",
                        url,
                        "--n-workers",
                        str(n_workers),
                        "--no-validation" if no_validation else None,
                        (
                            f"--tag-type-default={tag_type_default}"
                            if tag_type_default
                            else None
                        ),
                    ]
                )
                runner.invoke(cli, args, catch_exceptions=False)

        print("Removing duplicates ...")
        runner.invoke(
            tagstore, ["remove-duplicates", "-u", url], catch_exceptions=False
        )

        print("Refreshing db views ...")
        runner.invoke(tagstore, ["refresh-views", "-u", url], catch_exceptions=False)

        if not dont_update_quality_metrics:
            print("Calc Quality metrics ...")
            runner.invoke(
                cli, ["quality", "calculate", "-u", url], catch_exceptions=False
            )

        if run_cluster_mapping_with_env or rerun_cluster_mapping_with_env:
            print("Import cluster mappings ...")
            runner.invoke(
                tagstore,
                [
                    "insert_cluster_mappings",
                    "-u",
                    url,
                    "--use-gs-lib-config-env",
                    run_cluster_mapping_with_env or rerun_cluster_mapping_with_env,
                    "--update" if rerun_cluster_mapping_with_env else "",
                ],
                catch_exceptions=False,
            )

            print("Refreshing db views ...")
            runner.invoke(
                tagstore, ["refresh-views", "-u", url], catch_exceptions=False
            )

        print_success("Your tagstore is now up-to-date again.")

    else:
        print_fail(f"Repos to sync file {repos} does not exist.")


def validate_tagpack(config, path, no_address_validation):
    t0 = time.time()
    print_line("TagPack validation starts")
    print(f"Path: {path}")

    taxonomies = _load_taxonomies(config)
    taxonomy_keys = taxonomies.keys()
    print(f"Loaded taxonomies: {taxonomy_keys}")

    schema = TagPackSchema()
    print(f"Loaded schema: {schema.definition}")

    tagpack_files = collect_tagpack_files(path)
    n_tagpacks = len([f for fs in tagpack_files.values() for f in fs])
    print_info(f"Collected {n_tagpacks} TagPack files\n")

    no_passed = 0
    try:
        for headerfile_dir, files in tagpack_files.items():
            for tagpack_file in files:
                tagpack = TagPack.load_from_file(
                    "", tagpack_file, schema, taxonomies, headerfile_dir
                )

                print(f"{tagpack_file}: ", end="\n")

                tagpack.validate()
                # verify valid blocknetwork addresses using internal checksum
                if not no_address_validation:
                    tagpack.verify_addresses()

                print_success("PASSED")

                no_passed += 1
    except (ValidationError, TagPackFileError) as e:
        print_fail("FAILED", e)

    failed = no_passed < n_tagpacks

    status = "fail" if failed else "success"

    duration = round(time.time() - t0, 2)
    print_line(
        "{}/{} TagPacks passed in {}s".format(no_passed, n_tagpacks, duration), status
    )

    sys.exit(0 if not failed else 1)


def list_tags(url, schema, unique, category, network, csv):
    t0 = time.time()
    if not csv:
        print_line("List tags starts")

    tagstore = TagStore(url, schema)

    try:
        uniq, cat, net = unique, category, network
        qm = tagstore.list_tags(unique=uniq, category=cat, network=net)
        if not csv:
            print(f"{len(qm)} Tags found")
        else:
            print("network,tp_title,tag_label")
        for row in qm:
            print(("," if csv else ", ").join(map(str, row)))

        duration = round(time.time() - t0, 2)
        if not csv:
            print_line(f"Done in {duration}s", "success")
    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def _suggest_actors(url, schema, label, max_results):
    print_line(f"Searching suitable actors for {label} in TagStore")
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
    print("Starting interactive tagpack actor enrichment process.")

    tagstore = TagStore(url, schema)
    tagpack_files = collect_tagpack_files(path)

    schema_obj = TagPackSchema()
    user_choice_cache = {}

    for headerfile_dir, files in tagpack_files.items():
        for tagpack_file in files:
            tagpack = TagPack.load_from_file(
                "", tagpack_file, schema_obj, None, headerfile_dir
            )
            print(f"Loading {tagpack_file}: ")

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
                print_success(f"Writing updated Tagpack {updated_file}\n")
                with open(updated_file, "w") as outfile:
                    tagpack.contents["tags"] = tagpack.contents.pop(
                        "tags"
                    )  # re-insert tags
                    tagpack.update_lastmod()
                    yaml.dump(
                        tagpack.contents, outfile, sort_keys=False
                    )  # write in order of insertion
            else:
                print_success("No actors added, moving on.")


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
):
    t0 = time.time()
    print_line("TagPack insert starts")
    print(f"Path: {path}")

    if no_git:
        base_url = path
        print_line("No repository detection done.")
    else:
        base_url = get_repository(path)
        print_line(f"Detected repository root in {base_url}")

    tagstore = TagStore(url, schema)

    schema_obj = TagPackSchema()
    print_info(f"Loaded TagPack schema definition: {schema_obj.definition}")

    config_data = _load_config(config)
    taxonomies = _load_taxonomies(config_data)
    taxonomy_keys = taxonomies.keys()
    print(f"Loaded taxonomies: {taxonomy_keys}")

    tagpack_files = collect_tagpack_files(path)

    # resolve backlinks to remote repository and relative paths
    scheck, nogit = not no_strict_check, no_git
    prepared_packs = [
        (m, h, n[0], n[1], n[2])
        for m, h, n in [
            (a, h, get_uri_for_tagpack(base_url, a, scheck, nogit))
            for h, fs in tagpack_files.items()
            for a in fs
        ]
    ]

    prefix = None  # config.get("prefix", None)
    if add_new:  # don't re-insert existing tagpacks
        print_info("Checking which files are new to the tagstore:")
        prepared_packs = [
            (t, h, u, r, default_prefix)
            for (t, h, u, r, default_prefix) in prepared_packs
            if not tagstore.tp_exists(prefix if prefix else default_prefix, r)
        ]

    n_ppacks = len(prepared_packs)
    print_info(f"Collected {n_ppacks} TagPack files\n")

    packs = enumerate(sorted(prepared_packs), start=1)

    n_processes = n_workers if n_workers > 0 else cpu_count() + n_workers

    if n_processes < 1:
        print_fail(f"Can't use {n_processes} adjust your n_workers setting.")
        sys.exit(100)

    if n_processes > 1:
        print_info(f"Running parallel insert on {n_processes} workers.")

    worker = InsertTagpackWorker(
        url,
        schema,
        schema_obj,
        taxonomies,
        public,
        force,
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
    print_line(msg.format(no_passed, n_ppacks, no_tags, duration), status)
    msg = "Don't forget to run 'graphsense-cli tagstore refresh-views' soon to keep the database"
    msg += " consistent!"
    print_info(msg)


@cli.group("tagpack")
def tagpack():
    """commands regarding tags and tagpacks"""
    pass


@tagpack.command()
@click.argument("path", default=os.getcwd())
@click.option(
    "--no-address-validation",
    is_flag=True,
    help="Disables checksum validation of addresses",
)
@click.pass_context
def validate(ctx, path, no_address_validation):
    """validate TagPacks"""
    config = _load_config(ctx.obj["config"])
    validate_tagpack(config, path, no_address_validation)


@tagpack.command()
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
def list(schema, url, unique, category, network, csv):
    """list Tags"""
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
    list_tags(url, schema, unique, category, network, csv)


@tagpack.command()
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
@click.pass_context
def insert(
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
):
    """insert TagPacks"""
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
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
        ctx.obj["config"],
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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
    add_actors_to_tagpack(url, schema, path, max, categories, inplace)


def validate_actorpack(config, path):
    t0 = time.time()
    print_line("ActorPack validation starts")
    print(f"Path: {path}")

    taxonomies = _load_taxonomies(config)
    taxonomy_keys = taxonomies.keys()
    print(f"Loaded taxonomies: {taxonomy_keys}")

    schema = ActorPackSchema()
    print(f"Loaded schema: {schema.definition}")

    actorpack_files = collect_tagpack_files(path, search_actorpacks=True)
    n_actorpacks = len([f for fs in actorpack_files.values() for f in fs])
    print_info(f"Collected {n_actorpacks} ActorPack files\n")

    no_passed = 0
    try:
        for headerfile_dir, files in actorpack_files.items():
            for actorpack_file in files:
                actorpack = ActorPack.load_from_file(
                    "", actorpack_file, schema, taxonomies, headerfile_dir
                )

                print(f"{actorpack_file}:\n", end="")

                actorpack.validate()
                print_success("PASSED")

                no_passed += 1
    except (ValidationError, TagPackFileError, ParserError, ScannerError) as e:
        print_fail("FAILED", e)

    failed = no_passed < n_actorpacks

    status = "fail" if failed else "success"

    duration = round(time.time() - t0, 2)
    msg = f"{no_passed}/{n_actorpacks} ActorPacks passed in {duration}s"
    print_line(msg, status)

    sys.exit(0 if not failed else 1)


def insert_actorpacks(
    url, schema, path, batch_size, force, add_new, no_strict_check, no_git, config
):
    t0 = time.time()
    print_line("ActorPack insert starts")
    print(f"Path: {path}")

    if no_git:
        base_url = path
        print_line("No repository detection done.")
    else:
        base_url = get_repository(path)
        print_line(f"Detected repository root in {base_url}")

    tagstore = TagStore(url, schema)

    schema_obj = ActorPackSchema()
    print_info(f"Loaded ActorPack schema definition: {schema_obj.definition}")

    config_data = _load_config(config)
    taxonomies = _load_taxonomies(config_data)
    taxonomy_keys = taxonomies.keys()
    print(f"Loaded taxonomies: {taxonomy_keys}")

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
        print_info("Checking which ActorPacks are new to the tagstore:")
        prepared_packs = [
            (t, h, u, r, default_prefix)
            for (t, h, u, r, default_prefix) in prepared_packs
            if not tagstore.actorpack_exists(prefix if prefix else default_prefix, r)
        ]

    n_ppacks = len(prepared_packs)
    print_info(f"Collected {n_ppacks} ActorPack files\n")

    no_passed = 0
    no_actors = 0

    for i, pack in enumerate(sorted(prepared_packs), start=1):
        actorpack_file, headerfile_dir, uri, relpath, default_prefix = pack

        actorpack = ActorPack.load_from_file(
            uri, actorpack_file, schema_obj, taxonomies, headerfile_dir
        )

        print(f"{i} {actorpack_file}: ", end="")
        try:
            tagstore.insert_actorpack(
                actorpack, force, prefix if prefix else default_prefix, relpath
            )
            print_success(f"PROCESSED {len(actorpack.actors)} Actors")
            no_passed += 1
            no_actors += len(actorpack.actors)
        except Exception as e:
            print_fail("FAILED", e)

    status = "fail" if no_passed < n_ppacks else "success"

    duration = round(time.time() - t0, 2)
    msg = "Processed {}/{} ActorPacks with {} Actors in {}s."
    print_line(msg.format(no_passed, n_ppacks, no_actors, duration), status)


def list_actors(url, schema, category, csv):
    t0 = time.time()
    if not csv:
        print_line("List actors starts")

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
            print_line(f"Done in {duration}s", "success")
    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def list_address_actors(url, schema, network, csv):
    t0 = time.time()
    if not csv:
        print_line("List addresses with actor tags starts")

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
            print_line(f"Done in {duration}s", "success")
    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


@cli.group("actorpack")
def actorpack():
    """commands regarding actor information"""
    pass


@actorpack.command()
@click.argument("path", default=os.getcwd())
@click.pass_context
def validate(ctx, path):  # noqa: F811
    """validate ActorPacks"""
    config = _load_config(ctx.obj["config"])
    validate_actorpack(config, path)


@actorpack.command()
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
def insert(ctx, path, schema, url, batch_size, force, add_new, no_strict_check, no_git):  # noqa: F811
    """insert ActorPacks"""
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
    insert_actorpacks(
        url,
        schema,
        path,
        batch_size,
        force,
        add_new,
        no_strict_check,
        no_git,
        ctx.obj["config"],
    )


@actorpack.command()
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--category", default="", help="List Actors of a specific category")
@click.option("--csv", is_flag=True, help="Show csv output.")
def list(schema, url, category, csv):  # noqa: F811
    """list Actors"""
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
    list_address_actors(url, schema, network, csv)


def list_taxonomies(config):
    print_line("Show configured taxonomies")
    count = 0
    if "taxonomies" not in config:
        print_line("No configured taxonomies", "fail")
    else:
        for key, value in config["taxonomies"].items():
            print_info(value)
            count += 1
        print_line(f"{count} configured taxonomies", "success")


def show_taxonomy_concepts(config, taxonomy, tree, verbose):
    from anytree import RenderTree

    if "taxonomies" not in config:
        print_line("No taxonomies configured", "fail")
        return

    print_line("Showing concepts of taxonomy {}".format(taxonomy))
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
        print_line(f"{len(taxonomy_obj.concepts)} taxonomy concepts", "success")


def insert_taxonomy():
    print_line(
        "tagpack-tool taxonomy insert was"
        " retired in favor of tagstore init, please use this command",
        "fail",
    )
    sys.exit(1)


@cli.group("taxonomy")
@click.pass_context
def taxonomy(ctx):
    """taxonomy commands"""
    # Default behavior when no subcommand is provided
    if ctx.invoked_subcommand is None:
        config = _load_config(ctx.obj["config"])
        list_taxonomies(config)


@taxonomy.command()
@click.pass_context
def list(ctx):  # noqa: F811
    """list taxonomy concepts"""
    config = _load_config(ctx.obj["config"])
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
    config = _load_config(ctx.obj["config"])
    show_taxonomy_concepts(config, taxonomy_key, tree, verbose)


@taxonomy.command()
@click.argument(
    "taxonomy_key",
    type=click.Choice(["abuse", "entity", "confidence", "country", "concept"]),
    required=False,
)
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for taxonomy tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
def insert(taxonomy_key, schema, url):  # noqa: F811
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
        print_fail(
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
                    print_fail(
                        f"Environment {args.use_gs_lib_config_env} "
                        "not found in gs-config"
                    )
                    sys.exit(1)

        else:
            print_fail("Graphsense config not found at ~/.graphsense.yaml")
            sys.exit(1)
    else:
        if args.ks_file and os.path.exists(args.ks_file):
            return json.load(open(args.ks_file))
        else:
            print_fail(f"Keyspace config file not found at {args.ks_file}")
            sys.exit(1)


def init_db():
    print_line(
        "tagpack-tool init was retired"
        " in favor of tagstore init, please use this command",
        "fail",
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
    # Create a mock args object for compatibility with existing functions
    class MockArgs:
        def __init__(self):
            self.url = url
            self.schema = schema
            self.db_nodes = db_nodes
            self.cassandra_username = cassandra_username
            self.cassandra_password = cassandra_password
            self.ks_file = ks_file
            self.use_gs_lib_config_env = use_gs_lib_config_env
            self.update = update

    args = MockArgs()

    t0 = time.time()
    tagstore = TagStore(url, schema)
    df = pd.DataFrame(tagstore.get_addresses(update), columns=["address", "network"])
    ks_mapping = load_ks_mapping(args)
    print("Importing with mapping config: ", ks_mapping)
    networks = ks_mapping.keys()
    gs = GraphSense(
        db_nodes,
        ks_mapping,
        username=cassandra_username,
        password=cassandra_password,
    )

    workpackages = []
    for network, data in df.groupby("network"):
        if gs.contains_keyspace_mapping(network):
            for batch in _split_into_chunks(data, batch_size):
                workpackages.append((network, ks_mapping, args, batch))

    nr_workers = int(cpu_count() / 2)
    print(
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
        print_success(f"INSERTED/UPDATED {mappings_count} {pc} cluster mappings")

    tagstore.finish_mappings_update(networks)
    duration = round(time.time() - t0, 2)
    print_line(
        f"Inserted {'missing' if not update else 'all'} cluster mappings "
        f"for {processed_networks} in {duration}s",
        "success",
    )


def update_db(url, schema):
    tagstore = TagStore(url, schema)
    tagstore.refresh_db()
    print_info("All relevant views have been updated.")


def _remove_duplicates(url, schema):
    tagstore = TagStore(url, schema)
    rows_deleted = tagstore.remove_duplicates()
    msg = f"{rows_deleted} duplicate tags have been deleted from the database."
    print_info(msg)


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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
    show_tagstore_composition(url, schema, csv, by_network)


@tagstore.command()
@click.option(
    "--schema", default=DEFAULT_SCHEMA, help="PostgreSQL schema for tagpack tables"
)
@click.option("-u", "--url", help="postgresql://user:password@db_host:port/database")
@click.option("--csv", is_flag=True, help="Show csv output.")
def show_source_repos(schema, url, csv):
    """Shows which repos sources are stored in the database."""
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)
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
    print_line("Show quality measures")
    tagstore = TagStore(url, schema)

    try:
        qm = tagstore.get_quality_measures(network)
        c = network if network else "Global"
        print(f"{c} quality measures:")
        print_quality_measures(qm)

    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def calc_quality_measures(url, schema):
    t0 = time.time()
    print_line("Calculate quality measures starts")

    tagstore = TagStore(url, schema)

    try:
        qm = tagstore.calculate_quality_measures()
        print("Global quality measures:")
        print_quality_measures(qm)

        duration = round(time.time() - t0, 2)
        print_line(f"Done in {duration}s", "success")
    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def low_quality_addresses(url, schema, threshold, network, category, csv, cluster):
    if not csv:
        print_line("Addresses with low quality")
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
        print_fail(e)
        print_line("Operation failed", "fail")


def list_low_quality_actors(url, schema, category, max_results, not_used, csv):
    tagstore = TagStore(url, schema)

    res = tagstore.get_actors_with_jurisdictions(
        category=category, max_results=max_results, include_not_used=not_used
    )
    df = pd.DataFrame(res)
    if csv:
        print(df.to_csv(header=True, sep=",", index=True))
    else:
        print_line("Actors without Jurisdictions")
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
        print_line("Top labels without actor")
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
        print_line("Addresses with actor collisions")
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
    def_url, url_msg = read_url_from_env()
    if not url:
        url = def_url
    if not url:
        print_warn(url_msg)
        click.echo("No postgresql URL connection was provided. Exiting.")
        sys.exit(1)

    ctx.obj["url"] = url
    ctx.obj["schema"] = schema
    ctx.obj["network"] = network

    # Default behavior when no subcommand is provided
    if ctx.invoked_subcommand is None:
        show_quality_measures(url, schema, network)


@quality.command()
@click.pass_context
def calculate(ctx):
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
            print_warn(url_msg)
        e.show()
        sys.exit(1)


if __name__ == "__main__":
    main()
