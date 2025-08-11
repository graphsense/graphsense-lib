import json
import os
import subprocess
import sys
import tempfile
import time
from argparse import ArgumentParser
from functools import partial
from multiprocessing import Pool, cpu_count

import pandas as pd
import yaml
from anytree import RenderTree

# colorama fixes issues with redirecting colored outputs to files
from colorama import init
from git import Repo
from tabulate import tabulate
from yaml.parser import ParserError, ScannerError

from tagpack import get_version
from tagpack.actorpack import Actor, ActorPack
from tagpack.actorpack_schema import ActorPackSchema
from tagpack.cmd_utils import (
    print_fail,
    print_info,
    print_line,
    print_success,
    print_warn,
)
from tagpack.graphsense import GraphSense
from tagpack.tagpack import (
    TagPack,
    TagPackFileError,
    collect_tagpack_files,
    get_repository,
    get_uri_for_tagpack,
)
from tagpack.tagpack_schema import TagPackSchema, ValidationError
from tagpack.tagstore import InsertTagpackWorker, TagStore
from tagpack.taxonomy import Taxonomy
from tagpack.utils import strip_empty

init()

CONFIG_FILE = "config.yaml"

DEFAULT_CONFIG = {
    "taxonomies": {
        "concept": "src/tagpack/db/concepts.yaml",
        "confidence": "src/tagpack/db/confidence.csv",
        "country": "src/tagpack/db/countries.csv",
        "tag_type": "src/tagpack/db/tag_types.csv",
        "tag_subject": "src/tagpack/db/tag_subject.csv",
        "concept_relation_annotation": "src/tagpack/db/concept_relation_annotation.csv",
    }
}


_DEFAULT_SCHEMA = "public"


def _load_taxonomies(config):
    if "taxonomies" not in config:
        return None
    return {key: _load_taxonomy(config, key) for key in config["taxonomies"]}


def _load_taxonomy(config, key):
    if "taxonomies" not in config:
        return None
    uri = config["taxonomies"][key]
    taxonomy = Taxonomy(key, uri)
    remote = uri.startswith("http")
    if remote:
        taxonomy.load_from_remote()
    else:
        taxonomy.load_from_local()
    return taxonomy


def list_taxonomies(args=None):
    config = _load_config(args.config)

    print_line("Show configured taxonomies")
    count = 0
    if "taxonomies" not in config:
        print_line("No configured taxonomies", "fail")
    else:
        for key, value in config["taxonomies"].items():
            print_info(value)
            count += 1
        print_line(f"{count} configured taxonomies", "success")


def show_taxonomy_concepts(args, remote=False):
    config = _load_config(args.config)

    if "taxonomies" not in config:
        print_line("No taxonomies configured", "fail")
        return

    print_line("Showing concepts of taxonomy {}".format(args.taxonomy))
    uri = config["taxonomies"][args.taxonomy]
    print(f"URI: {uri}\n")
    taxonomy = _load_taxonomy(config, args.taxonomy)
    if args.tree:
        for pre, fill, node in RenderTree(taxonomy.get_concept_tree()):
            print("%s%s" % (pre, node.name))
    else:
        if args.verbose:
            headers = ["Id", "Label", "Level", "Uri", "Description"]
            table = [
                [c.id, c.label, c.level, c.uri, c.description]
                for c in taxonomy.concepts
            ]
        elif args.taxonomy == "confidence":
            headers = ["Level", "Label"]
            table = [[c.level, c.label] for c in taxonomy.concepts]
        else:
            headers = ["Id", "Label"]
            table = [[c.id, c.label] for c in taxonomy.concepts]

        print(tabulate(table, headers=headers))
        print_line(f"{len(taxonomy.concepts)} taxonomy concepts", "success")


def insert_taxonomy(args, remote=False):
    print_line(
        "tagpack-tool taxonomy insert was"
        " retired in favor of tagstore init, please use this command",
        "fail",
    )
    sys.exit(1)


def low_quality_addresses(args):
    if not args.csv:
        print_line("Addresses with low quality")
    tagstore = TagStore(args.url, args.schema)

    try:
        th, curr, cat = args.threshold, args.network, args.category
        la = tagstore.low_quality_address_labels(th, curr, cat)
        if la:
            if not args.csv:
                c = args.network if args.network else "all"
                print(f"List of {c} addresses and labels ({len(la)}):")
            else:
                print("network,address,labels")

            intersections = []
            for (network, address), labels in la.items():
                if args.csv:
                    labels_str = "|".join(labels)
                    print(f"{network},{address},{labels_str}")
                else:
                    print(f"\t{network}\t{address}\t{labels}")

                if not args.cluster:
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

            if args.cluster:
                print("\nSets of tags appearing in several addresses:")
                s_int = sorted(intersections, key=lambda x: x[1], reverse=True)
                for k, v in s_int:
                    if v > 1:
                        print(f"\t{v}: {', '.join(k)}")
        else:
            if not args.csv:
                print("\tNone")

    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


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


def show_quality_measures(args):
    print_line("Show quality measures")
    tagstore = TagStore(args.url, args.schema)

    try:
        qm = tagstore.get_quality_measures(args.network)
        c = args.network if args.network else "Global"
        print(f"{c} quality measures:")
        print_quality_measures(qm)

    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def calc_quality_measures(args):
    t0 = time.time()
    print_line("Calculate quality measures starts")

    tagstore = TagStore(args.url, args.schema)

    try:
        qm = tagstore.calculate_quality_measures()
        print("Global quality measures:")
        print_quality_measures(qm)

        duration = round(time.time() - t0, 2)
        print_line(f"Done in {duration}s", "success")
    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def _load_config(cfile):
    if not os.path.isfile(cfile):
        return DEFAULT_CONFIG
    return yaml.safe_load(open(cfile, "r"))


def show_config(args):
    if os.path.exists(args.config):
        print("Using Config File:", args.config)
    else:
        print_info(
            f"No override config file found at {args.config}. Using default values."
        )
    if args.verbose:
        list_taxonomies(args)


def validate_tagpack(args):
    config = _load_config(args.config)

    t0 = time.time()
    print_line("TagPack validation starts")
    print(f"Path: {args.path}")

    taxonomies = _load_taxonomies(config)
    taxonomy_keys = taxonomies.keys()
    print(f"Loaded taxonomies: {taxonomy_keys}")

    schema = TagPackSchema()
    print(f"Loaded schema: {schema.definition}")

    tagpack_files = collect_tagpack_files(args.path)
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
                if not args.no_address_validation:
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


def suggest_actors(args):
    print_line(f"Searching suitable actors for {args.label} in TagStore")
    tagstore = TagStore(args.url, args.schema)
    candidates = tagstore.find_actors_for(
        args.label, args.max, use_simple_similarity=False, threshold=0.1
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


def add_actors_to_tagpack(args):
    print("Starting interactive tagpack actor enrichment process.")

    tagstore = TagStore(args.url, args.schema)
    tagpack_files = collect_tagpack_files(args.path)

    schema = TagPackSchema()
    user_choice_cache = {}

    for headerfile_dir, files in tagpack_files.items():
        for tagpack_file in files:
            tagpack = TagPack.load_from_file(
                "", tagpack_file, schema, None, headerfile_dir
            )
            print(f"Loading {tagpack_file}: ")

            def find_actor_candidates(search_term):
                res = tagstore.find_actors_for(
                    search_term,
                    args.max,
                    use_simple_similarity=False,
                    threshold=0.1,
                )

                def get_label(actor_row):
                    a = Actor.from_contents(actor_row, None)
                    return f"{actor_row['label']} ({', '.join(a.uris)})"

                return [(x["id"], get_label(x)) for x in res]

            category_filter = strip_empty(args.categories.split(","))
            updated = tagpack.add_actors(
                find_actor_candidates,
                only_categories=category_filter if len(category_filter) > 0 else None,
                user_choice_cache=user_choice_cache,
            )

            if updated:
                updated_file = (
                    tagpack_file.replace(".yaml", "_with_actors.yaml")
                    if not args.inplace
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


def insert_tagpack(args):
    t0 = time.time()
    print_line("TagPack insert starts")
    print(f"Path: {args.path}")

    if args.no_git:
        base_url = args.path
        print_line("No repository detection done.")
    else:
        base_url = get_repository(args.path)
        print_line(f"Detected repository root in {base_url}")

    tagstore = TagStore(args.url, args.schema)

    schema = TagPackSchema()
    print_info(f"Loaded TagPack schema definition: {schema.definition}")

    config = _load_config(args.config)
    taxonomies = _load_taxonomies(config)
    taxonomy_keys = taxonomies.keys()
    print(f"Loaded taxonomies: {taxonomy_keys}")

    tagpack_files = collect_tagpack_files(args.path)

    # resolve backlinks to remote repository and relative paths
    scheck, nogit = not args.no_strict_check, args.no_git
    prepared_packs = [
        (m, h, n[0], n[1], n[2])
        for m, h, n in [
            (a, h, get_uri_for_tagpack(base_url, a, scheck, nogit))
            for h, fs in tagpack_files.items()
            for a in fs
        ]
    ]

    prefix = None  # config.get("prefix", None)
    if args.add_new:  # don't re-insert existing tagpacks
        print_info("Checking which files are new to the tagstore:")
        prepared_packs = [
            (t, h, u, r, default_prefix)
            for (t, h, u, r, default_prefix) in prepared_packs
            if not tagstore.tp_exists(prefix if prefix else default_prefix, r)
        ]

    n_ppacks = len(prepared_packs)
    print_info(f"Collected {n_ppacks} TagPack files\n")

    public, force = args.public, args.force

    # supported = tagstore.supported_currencies
    # for i, tp in enumerate(sorted(prepared_packs), start=1):
    #     tagpack_file, headerfile_dir, uri, relpath, default_prefix = tp

    packs = enumerate(sorted(prepared_packs), start=1)

    n_processes = args.n_workers if args.n_workers > 0 else cpu_count() + args.n_workers

    if n_processes < 1:
        print_fail(f"Can't use {n_processes} adjust your n_workers setting.")
        sys.exit(100)

    if n_processes > 1:
        print_info(f"Running parallel insert on {n_processes} workers.")

    worker = InsertTagpackWorker(
        args.url,
        args.schema,
        schema,
        taxonomies,
        public,
        force,
        validate_tagpack=not args.no_validation,
        tag_type_default=args.tag_type_default,
        no_git=args.no_git,
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
    # msg += "Only tags for supported currencies {} are inserted."
    print_line(msg.format(no_passed, n_ppacks, no_tags, duration), status)
    msg = "Don't forget to run 'tagstore refresh_views' soon to keep the database"
    msg += " consistent!"
    print_info(msg)


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


def insert_cluster_mapping(args, batch_size=5_000):
    t0 = time.time()
    tagstore = TagStore(args.url, args.schema)
    df = pd.DataFrame(
        tagstore.get_addresses(args.update), columns=["address", "network"]
    )
    ks_mapping = load_ks_mapping(args)
    print("Importing with mapping config: ", ks_mapping)
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
        f"Inserted {'missing' if not args.update else 'all'} cluster mappings "
        f"for {processed_networks} in {duration}s",
        "success",
    )


def init_db(args):
    print_line(
        "tagpack-tool init was retired"
        " in favor of tagstore init, please use this command",
        "fail",
    )
    sys.exit(1)


def update_db(args):
    tagstore = TagStore(args.url, args.schema)
    tagstore.refresh_db()
    print_info("All relevant views have been updated.")


def remove_duplicates(args):
    tagstore = TagStore(args.url, args.schema)
    rows_deleted = tagstore.remove_duplicates()
    msg = f"{rows_deleted} duplicate tags have been deleted from the database."
    print_info(msg)


def show_version():
    return f"GraphSense TagPack management tool {get_version()}"


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


def show_tagstore_composition(args):
    tagstore = TagStore(args.url, args.schema)
    headers = (
        ["creator", "group", "network", "labels_count", "tags_count"]
        if args.by_network
        else ["creator", "group", "labels_count", "tags_count"]
    )
    df = pd.DataFrame(
        tagstore.get_tagstore_composition(by_network=args.by_network), columns=headers
    )

    if args.csv:
        print(df.to_csv(header=True, sep=",", index=True))
    else:
        with pd.option_context(
            "display.max_rows", None, "display.max_columns", None
        ):  # more options can be specified also
            print(tabulate(df, headers=headers, tablefmt="psql"))


def validate_actorpack(args):
    config = _load_config(args.config)

    t0 = time.time()
    print_line("ActorPack validation starts")
    print(f"Path: {args.path}")

    taxonomies = _load_taxonomies(config)
    taxonomy_keys = taxonomies.keys()
    print(f"Loaded taxonomies: {taxonomy_keys}")

    schema = ActorPackSchema()
    print(f"Loaded schema: {schema.definition}")

    actorpack_files = collect_tagpack_files(args.path, search_actorpacks=True)
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


def insert_actorpacks(args):
    t0 = time.time()
    print_line("ActorPack insert starts")
    print(f"Path: {args.path}")

    if args.no_git:
        base_url = args.path
        print_line("No repository detection done.")
    else:
        base_url = get_repository(args.path)
        print_line(f"Detected repository root in {base_url}")

    tagstore = TagStore(args.url, args.schema)

    schema = ActorPackSchema()
    print_info(f"Loaded ActorPack schema definition: {schema.definition}")

    config = _load_config(args.config)
    taxonomies = _load_taxonomies(config)
    taxonomy_keys = taxonomies.keys()
    print(f"Loaded taxonomies: {taxonomy_keys}")

    actorpack_files = collect_tagpack_files(args.path, search_actorpacks=True)

    # resolve backlinks to remote repository and relative paths
    # For the URI we use the same logic for ActorPacks than for TagPacks
    scheck, nogit = not args.no_strict_check, args.no_git
    prepared_packs = [
        (m, h, n[0], n[1], n[2])
        for m, h, n in [
            (a, h, get_uri_for_tagpack(base_url, a, scheck, nogit))
            for h, fs in actorpack_files.items()
            for a in fs
        ]
    ]

    prefix = None  # config.get("prefix", None)
    if args.add_new:  # don't re-insert existing tagpacks
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
    force = args.force

    for i, pack in enumerate(sorted(prepared_packs), start=1):
        actorpack_file, headerfile_dir, uri, relpath, default_prefix = pack

        actorpack = ActorPack.load_from_file(
            uri, actorpack_file, schema, taxonomies, headerfile_dir
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


def list_actors(args):
    t0 = time.time()
    if not args.csv:
        print_line("List actors starts")

    tagstore = TagStore(args.url, args.schema)

    try:
        qm = tagstore.list_actors(category=args.category)
        if not args.csv:
            print(f"{len(qm)} Actors found")
        else:
            print("actorpack,actor_id,actor_label,concept_label")

        for row in qm:
            print(("," if args.csv else ", ").join(map(str, row)))

        duration = round(time.time() - t0, 2)
        if not args.csv:
            print_line(f"Done in {duration}s", "success")
    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def list_tags(args):
    t0 = time.time()
    if not args.csv:
        print_line("List tags starts")

    tagstore = TagStore(args.url, args.schema)

    try:
        uniq, cat, net = args.unique, args.category, args.network
        qm = tagstore.list_tags(unique=uniq, category=cat, network=net)
        if not args.csv:
            print(f"{len(qm)} Tags found")
        else:
            print("network,tp_title,tag_label")
        for row in qm:
            print(("," if args.csv else ", ").join(map(str, row)))

        duration = round(time.time() - t0, 2)
        if not args.csv:
            print_line(f"Done in {duration}s", "success")
    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def list_address_actors(args):
    t0 = time.time()
    if not args.csv:
        print_line("List addresses with actor tags starts")

    tagstore = TagStore(args.url, args.schema)

    try:
        qm = tagstore.list_address_actors(network=args.network)
        if not args.csv:
            print(f"{len(qm)} addresses found")
        else:
            print("tag_id,tag_label,tag_address,tag_category,actor_label")

        for row in qm:
            print((", " if not args.csv else ",").join(map(str, row)))

        duration = round(time.time() - t0, 2)
        if not args.csv:
            print_line(f"Done in {duration}s", "success")
    except Exception as e:
        print_fail(e)
        print_line("Operation failed", "fail")


def exec_cli_command(arguments):
    saved_argv = sys.argv
    try:
        sys.argv[1:] = arguments
        return main()
    finally:
        sys.argv = saved_argv


def sync_repos(args):
    if os.path.isfile(args.repos):
        with open(args.repos, "r") as f:
            repos = [x.strip() for x in f.readlines() if not x.startswith("#")]

        print_line("Init db and add taxonomies ...")
        subprocess.call(["gs-tagstore-cli", "init", "--db-url", args.url])

        extra_option = "--force" if args.force else None
        extra_option = "--add_new" if extra_option is None else extra_option

        for repo_url in repos:
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
                exec_cli_command(
                    strip_empty(["actorpack", "insert", temp_dir_tt, "-u", args.url])
                )

                print("Inserting tagpacks ...")
                public = len(branch_etc) > 1 and branch_etc[1].strip() == "public"

                tag_type_default = (
                    branch_etc[2].strip() if len(branch_etc) > 2 else None
                )

                if public:
                    print("Caution: This repo is imported as public.")

                exec_cli_command(
                    strip_empty(
                        [
                            "tagpack",
                            "insert",
                            extra_option,
                            "--public" if public else None,
                            temp_dir_tt,
                            "-u",
                            args.url,
                            "--n-workers",
                            str(args.n_workers),
                            "--no-validation" if args.no_validation else None,
                            (
                                f"--tag-type-default={tag_type_default}"
                                if tag_type_default
                                else None
                            ),
                        ]
                    )
                )

        print("Removing duplicates ...")
        exec_cli_command(["tagstore", "remove_duplicates", "-u", args.url])

        print("Refreshing db views ...")
        exec_cli_command(["tagstore", "refresh_views", "-u", args.url])

        if not args.dont_update_quality_metrics:
            print("Calc Quality metrics ...")
            exec_cli_command(["quality", "calculate", "-u", args.url])

        if args.run_cluster_mapping_with_env or args.rerun_cluster_mapping_with_env:
            print("Import cluster mappings ...")
            exec_cli_command(
                [
                    "tagstore",
                    "insert_cluster_mappings",
                    "-u",
                    args.url,
                    "--use-gs-lib-config-env",
                    args.run_cluster_mapping_with_env
                    or args.rerun_cluster_mapping_with_env,
                    "--update" if args.rerun_cluster_mapping_with_env else "",
                ]
            )

            print("Refreshing db views ...")
            exec_cli_command(["tagstore", "refresh_views", "-u", args.url])

        print_success("Your tagstore is now up-to-date again.")

    else:
        print_fail(f"Repos to sync file {args.repos} does not exist.")


def list_low_quality_actors(args):
    tagstore = TagStore(args.url, args.schema)

    res = tagstore.get_actors_with_jurisdictions(
        category=args.category, max_results=args.max, include_not_used=args.not_used
    )
    df = pd.DataFrame(res)
    if args.csv:
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


def list_top_labels_without_actor(args):
    tagstore = TagStore(args.url, args.schema)

    res = tagstore.top_labels_without_actor(
        category=args.category, max_results=args.max
    )
    df = pd.DataFrame(res)
    if args.csv:
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


def list_addresses_with_actor_collisions(args):
    tagstore = TagStore(args.url, args.schema)

    res = tagstore.addresses_with_actor_collisions()
    df = pd.DataFrame(res)
    if args.csv:
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


def show_tagstore_source_repos(args):
    tagstore = TagStore(args.url, args.schema)

    res = tagstore.tagstore_source_repos()
    df = pd.DataFrame(res)
    if args.csv:
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


def main():
    if sys.version_info < (3, 7):
        sys.exit("This program requires python version 3.7 or later")

    # Deprecation warning
    print_warn("DEPRECATION WARNING: 'tagpack-tool' is deprecated and will be removed in a future version.")
    print_warn("Please use 'graphsense-cli tagpack-tool' instead.")
    print()

    parser = ArgumentParser(
        description="GraphSense TagPack validation and insert tool",
        epilog="GraphSense TagPack Tool v{} - https://graphsense.info".format(
            get_version()
        ),
    )

    def set_print_help_on_error(parser):
        def print_help_subparser(subparser, args):
            subparser.print_help()
            print_fail("No action was requested. Please use as specified above.")

        parser.set_defaults(func=partial(print_help_subparser, parser))

    set_print_help_on_error(parser)

    parser.add_argument("-v", "--version", action="version", version=show_version())
    parser.add_argument(
        "--config",
        help="path to config.yaml",
        default=os.path.join(os.getcwd(), CONFIG_FILE),
    )

    # Read the default URL from the .env file
    def_url, url_msg = read_url_from_env()

    subparsers = parser.add_subparsers(title="Commands")

    # parser for config command
    parser_c = subparsers.add_parser("config", help="show repository config")
    parser_c.add_argument(
        "-v", "--verbose", action="store_true", help="verbose configuration"
    )
    parser_c.set_defaults(func=show_config)

    # parser for sync command
    parser_syc = subparsers.add_parser(
        "sync", help="syncs the tagstore with a list of git repos."
    )
    parser_syc.add_argument(
        "-r",
        "--repos",
        help="File with list of repos to sync to the database.",
        default=os.path.join(os.getcwd(), "tagpack-repos.config"),
    )
    parser_syc.add_argument(
        "--force",
        action="store_true",
        help=(
            "By default, tagpack/actorpack insertion stops when an already inserted"
            "tagpack/actorpack exists in the database. Use this switch to force "
            " re-insertion."
        ),
    )
    parser_syc.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    parser_syc.add_argument(
        "--run-cluster-mapping-with-env",
        help="Environment in graphsense-lib config"
        " to use for the mapping process."
        " Only inserts non existing mappings.",
    )
    parser_syc.add_argument(
        "--rerun-cluster-mapping-with-env",
        help="Environment in graphsense-lib config"
        " to use for the mapping process."
        " Reinserts all mappings.",
    )
    parser_syc.add_argument(
        "--n-workers",
        type=int,
        default=1,
        help=(
            "number of workers to use for the tagpack insert. "
            "Default is 1. Zero or negative values are used as"
            "offset of the machines cpu_count."
        ),
    )
    parser_syc.add_argument(
        "--no-validation",
        action="store_true",
        help="Do not validate tagpacks before insert. (better insert speed)",
    )
    parser_syc.add_argument(
        "--dont-update-quality-metrics",
        action="store_true",
        help="Do update quality metrinc. (better insert speed)",
    )
    parser_syc.set_defaults(func=sync_repos, url=def_url)

    # parsers for tagpack command
    parser_tp = subparsers.add_parser(
        "tagpack", help="commands regarding tags and tagpacks"
    )
    set_print_help_on_error(parser_tp)

    ptp = parser_tp.add_subparsers(title="TagPack commands")

    # parser for list tags command
    ptp_l = ptp.add_parser("list", help="list Tags")

    ptp_l.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for tagpack tables",
    )
    ptp_l.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    ptp_l.add_argument(
        "--unique",
        action="store_true",
        help="List Tags removing duplicates",
    )
    ptp_l.add_argument(
        "--category", default="", help="List Tags of a specific category"
    )
    ptp_l.add_argument(
        "--network",
        default="",
        help="List Tags of a specific crypto-currency network",
    )
    ptp_l.add_argument("--csv", action="store_true", help="Show csv output.")
    ptp_l.set_defaults(func=list_tags, url=def_url)

    # parser for validate command
    ptp_v = ptp.add_parser("validate", help="validate TagPacks")
    ptp_v.add_argument(
        "path",
        nargs="?",
        metavar="PATH",
        default=os.getcwd(),
        help="TagPack file or folder root path (current folder by \
                    default)",
    )
    ptp_v.add_argument(
        "--no_address_validation",
        action="store_true",
        help="Disables checksum validation of addresses",
    )
    ptp_v.set_defaults(func=validate_tagpack)

    # parser for insert command
    ptp_i = ptp.add_parser("insert", help="insert TagPacks")
    ptp_i.add_argument(
        "path",
        nargs="?",
        metavar="PATH",
        default=os.getcwd(),
        help="TagPacks file or folder root path",
    )
    ptp_i.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for tagpack tables",
    )
    ptp_i.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    ptp_i.add_argument(
        "-b",
        "--batch_size",
        nargs="?",
        type=int,
        default=1000,
        help="batch size for insert)",
    )
    ptp_i.add_argument(
        "--public",
        action="store_true",
        help="By default, tagpacks are declared private in the database.\
                    Use this switch to declare them public.",
    )
    ptp_i.add_argument(
        "--force",
        action="store_true",
        help="By default, tagpack insertion stops when an already inserted\
                    tagpack exists in the database. Use this switch to force \
                    re-insertion.",
    )
    ptp_i.add_argument(
        "--add_new",
        action="store_true",
        help="By default, tagpack insertion stops when an already inserted\
                    tagpack exists in the database. Use this switch to insert \
                    new tagpacks while skipping over existing ones.",
    )
    ptp_i.add_argument(
        "--no_strict_check",
        action="store_true",
        help="Disables check for local modifications in git repository",
    )
    ptp_i.add_argument(
        "--no_git", action="store_true", help="Disables check for local git repository"
    )
    ptp_i.add_argument(
        "--n-workers",
        type=int,
        default=1,
        help=(
            "number of workers to use for the tagpack insert. "
            "Default is 1. Zero or negative values are used as"
            "offset of the machines cpu_count."
        ),
    )
    ptp_i.add_argument(
        "--no-validation",
        action="store_true",
        help="Do not validate tagpacks before insert. (better insert speed)",
    )
    ptp_i.add_argument(
        "--tag-type-default",
        type=str,
        default="actor",
        help=(
            "Default value for tag-type if missing in the tagpack. "
            "Default is legacy value actor."
        ),
    )
    ptp_i.set_defaults(func=insert_tagpack, url=def_url)

    # parser for suggest_actor
    ptp_actor = ptp.add_parser("suggest_actors", help="suggest an actor based on input")
    ptp_actor.add_argument(
        "label",
        nargs="?",
        help="label string to find actor suggestions for",
    )
    ptp_actor.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for tagpack tables",
    )
    ptp_actor.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    ptp_actor.add_argument(
        "--max",
        default=5,
        help="Limits the number of results",
    )
    ptp_actor.set_defaults(func=suggest_actors, url=def_url)

    # parser for add_actor
    ptp_add_actor = ptp.add_parser(
        "add_actors", help="interactively add actors to tagpack"
    )
    ptp_add_actor.add_argument(
        "path",
        nargs="?",
        metavar="PATH",
        default=os.getcwd(),
        help="TagPacks file or folder root path",
    )
    ptp_add_actor.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for tagpack tables",
    )
    ptp_add_actor.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    ptp_add_actor.add_argument(
        "--max",
        default=5,
        help="Limits the number of results",
    )
    ptp_add_actor.add_argument(
        "--categories",
        default="",
        help="Only edit tags of a certain category (multiple possible with semi-colon)",
    )
    ptp_add_actor.add_argument(
        "--inplace",
        action="store_true",
        help="If set the source tagpack file is overwritten, "
        "otherwise a new file is generated called [original_file]_with_actors.yaml.",
    )
    ptp_add_actor.set_defaults(func=add_actors_to_tagpack, url=def_url)

    # parsers for actorpack command
    parser_ap = subparsers.add_parser(
        "actorpack", help="commands regarding actor information"
    )
    set_print_help_on_error(parser_ap)

    app = parser_ap.add_subparsers(title="ActorPack commands")

    # parser for list actors command
    app_l = app.add_parser("list", help="list Actors")

    app_l.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for tagpack tables",
    )
    app_l.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    app_l.add_argument(
        "--category", default="", help="List Actors of a specific category"
    )
    app_l.add_argument("--csv", action="store_true", help="Show csv output.")
    app_l.set_defaults(func=list_actors, url=def_url)

    # parser for list addresses with actor-tags command
    app_a = app.add_parser("list_address_actor", help="list addresses-actors")

    app_a.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for tagpack tables",
    )
    app_a.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    app_a.add_argument(
        "--network",
        default="",
        help="List addresses of a specific crypto-currency network",
    )
    app_a.add_argument("--csv", action="store_true", help="Show csv output.")
    app_a.set_defaults(func=list_address_actors, url=def_url)

    # parser for validate command
    app_v = app.add_parser("validate", help="validate ActorPacks")
    app_v.add_argument(
        "path",
        nargs="?",
        metavar="PATH",
        default=os.getcwd(),
        help="ActorPack file or folder root path (current folder by default)",
    )
    app_v.set_defaults(func=validate_actorpack)

    # parser for insert command
    app_i = app.add_parser("insert", help="insert ActorPacks")
    app_i.add_argument(
        "path",
        nargs="?",
        metavar="PATH",
        default=os.getcwd(),
        help="ActorPacks file or folder root path",
    )
    app_i.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for actorpack tables",
    )
    app_i.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    app_i.add_argument(
        "-b",
        "--batch_size",
        nargs="?",
        type=int,
        default=1000,
        help="batch size for insert",
    )
    app_i.add_argument(
        "--force",
        action="store_true",
        help="By default, actorpack insertion stops when an already inserted \
                actorpack exists in the database. Use this switch to force \
                re-insertion.",
    )
    app_i.add_argument(
        "--add_new",
        action="store_true",
        help="By default, actorpack insertion stops when an already inserted \
                actorpack exists in the database. Use this switch to insert \
                new actorpacks while skipping over existing ones.",
    )
    app_i.add_argument(
        "--no_strict_check",
        action="store_true",
        help="Disables check for local modifications in git repository",
    )
    app_i.add_argument(
        "--no_git", action="store_true", help="Disables check for local git repository"
    )
    app_i.set_defaults(func=insert_actorpacks, url=def_url)

    # parser for taxonomy command
    parser_t = subparsers.add_parser("taxonomy", help="taxonomy commands")
    parser_t.set_defaults(func=list_taxonomies)

    pxp = parser_t.add_subparsers(title="Taxonomy commands")

    # parser for taxonomy list command
    pxp_l = pxp.add_parser("list", help="list taxonomy concepts")
    pxp_l.set_defaults(func=list_taxonomies)

    # parser for taxonomy show command
    pxp_s = pxp.add_parser("show", help="show taxonomy concepts")
    pxp_s.add_argument(
        "taxonomy",
        metavar="TAXONOMY_KEY",
        choices=["abuse", "entity", "confidence", "country", "concept"],
        help="the selected taxonomy",
    )
    pxp_s.add_argument("-v", "--verbose", action="store_true", help="verbose concepts")
    pxp_s.add_argument("--tree", action="store_true", help="Show as tree")
    pxp_s.set_defaults(func=show_taxonomy_concepts)

    # parser for taxonomy insert command
    pxp_i = pxp.add_parser("insert", help="insert taxonomy into GraphSense")
    pxp_i.add_argument(
        "taxonomy",
        metavar="TAXONOMY_KEY",
        nargs="?",
        choices=["abuse", "entity", "confidence", "country", "concept"],
        default=None,
        help="the selected taxonomy",
    )
    pxp_i.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for taxonomy tables",
    )
    pxp_i.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pxp_i.set_defaults(func=insert_taxonomy, url=def_url)

    # parsers for database housekeeping
    parser_db = subparsers.add_parser("tagstore", help="database housekeeping commands")
    set_print_help_on_error(parser_db)

    pdp = parser_db.add_subparsers(title="TagStore commands")

    # init the database
    pbp = pdp.add_parser("init", help="init the database")
    pbp.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for GraphSense cluster mapping table",
    )
    pbp.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pbp.set_defaults(func=init_db, url=def_url, taxonomy=None)

    # insert_cluster_mappings [update]
    pc = pdp.add_parser("insert_cluster_mappings", help="insert cluster mappings")
    pc.add_argument(
        "-d",
        "--db_nodes",
        nargs="+",
        default=["localhost"],
        metavar="DB_NODE",
        help='Cassandra node(s); default "localhost")',
    )
    pc.add_argument(
        "--cassandra-username",
        default=None,
        metavar="USERNAME",
        help="Cassandra Username",
    )
    pc.add_argument(
        "--cassandra-password",
        default=None,
        metavar="PASSWORD",
        help="Cassandra password",
    )
    pc.add_argument(
        "-f",
        "--ks_file",
        metavar="KEYSPACE_FILE",
        help="JSON file with Cassandra keyspaces that contain GraphSense \
                    cluster mappings",
    )
    pc.add_argument(
        "--use-gs-lib-config-env",
        metavar="GS_LIB_ENV",
        help="Load ks-mapping from global graphsense-lib config."
        " Overrides --ks_file and --db_nodes",
    )
    pc.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for GraphSense cluster mapping table",
    )
    pc.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pc.add_argument("--update", action="store_true", help="update all cluster mappings")
    pc.set_defaults(func=insert_cluster_mapping, url=def_url)

    # refresh_views
    pd = pdp.add_parser("refresh_views", help="update views")
    pd.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for GraphSense cluster mapping table",
    )
    pd.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pd.set_defaults(func=update_db, url=def_url)

    # remove_duplicates
    pr = pdp.add_parser("remove_duplicates", help="remove duplicate tags")
    pr.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for GraphSense cluster mapping table",
    )
    pr.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pr.set_defaults(func=remove_duplicates, url=def_url)

    # show composition summary of the current tagstore.
    psc = pdp.add_parser(
        "show_composition",
        help="Shows the tag composition grouped by creator and category.",
    )
    psc.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for tagpack tables",
    )
    psc.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    psc.add_argument("--csv", action="store_true", help="Show csv output.")
    psc.add_argument(
        "--by-network",
        action="store_true",
        help="Include currency/network in statistic.",
    )
    psc.set_defaults(func=show_tagstore_composition, url=def_url)

    # show composition repos ingested in the db.
    psir = pdp.add_parser(
        "show_source_repos",
        help="Shows which repos sources are stored in the database.",
    )
    psir.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for tagpack tables",
    )
    psir.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    psir.add_argument("--csv", action="store_true", help="Show csv output.")
    psir.set_defaults(func=show_tagstore_source_repos, url=def_url)

    # parser for quality measures
    parser_q = subparsers.add_parser("quality", help="calculate tags quality measures")
    parser_q.set_defaults(
        func=show_quality_measures, url=def_url, schema=_DEFAULT_SCHEMA, network=""
    )

    pqp = parser_q.add_subparsers(title="Quality commands")

    # parser for quality measures calculation
    pqp_i = pqp.add_parser(
        "calculate", help="calculate quality measures for all tags in the DB"
    )
    pqp_i.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for quality measures tables",
    )
    pqp_i.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pqp_i.set_defaults(func=calc_quality_measures, url=def_url)

    # parser for quality measures list
    pqp_l = pqp.add_parser(
        "list_addresses_with_low_quality", help="list low quality addresses"
    )
    pqp_l.add_argument(
        "--category", default="", help="List addresses of a specific category"
    )
    pqp_l.add_argument(
        "--network",
        default="",
        help="Show low quality addresses of a specific crypto-currency network",
    )
    pqp_l.add_argument(
        "--threshold",
        default=0.25,
        help="List addresses having a quality lower than this threshold",
    )
    pqp_l.add_argument(
        "-c",
        "--cluster",
        action="store_true",
        help="Cluster addresses having intersections of similar tags",
    )
    pqp_l.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for quality measures tables",
    )
    pqp_l.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pqp_l.add_argument("--csv", action="store_true", help="Show csv output.")
    pqp_l.set_defaults(func=low_quality_addresses, url=def_url)

    # parser for actors missing Jur
    pqp_j = pqp.add_parser(
        "list_actors_without_jur", help="actors without jurisdictions."
    )
    pqp_j.add_argument(
        "--category", default="", help="List actors of a specific category"
    )
    pqp_j.add_argument(
        "--max",
        default=5,
        help="Limits the number of results",
    )
    pqp_j.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for quality measures tables",
    )
    pqp_j.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pqp_j.add_argument("--csv", action="store_true", help="Show csv output.")
    pqp_j.add_argument(
        "--not_used",
        action="store_true",
        help="Include actors that are not used in tags.",
    )
    pqp_j.set_defaults(func=list_low_quality_actors, url=def_url)

    # parser top labels with no actor
    pqp_j = pqp.add_parser(
        "list_labels_without_actor",
        help="List the top labels used in tags without actors.",
    )
    pqp_j.add_argument(
        "--category", default="", help="List actors of a specific category"
    )
    pqp_j.add_argument(
        "--max",
        default=5,
        help="Limits the number of results",
    )
    pqp_j.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for quality measures tables",
    )
    pqp_j.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pqp_j.add_argument("--csv", action="store_true", help="Show csv output.")
    pqp_j.set_defaults(func=list_top_labels_without_actor, url=def_url)

    # parser top labels with no actor
    pqp_j = pqp.add_parser(
        "list_addresses_with_actor_collisions",
        help="List actors with address collisions.",
    )
    pqp_j.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for quality measures tables",
    )
    pqp_j.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pqp_j.add_argument("--csv", action="store_true", help="Show csv output.")
    pqp_j.set_defaults(func=list_addresses_with_actor_collisions, url=def_url)

    # parser for quality measures show
    pqp_s = pqp.add_parser("show", help="show average quality measures")
    pqp_s.add_argument(
        "--network",
        default="",
        help="Show the avg quality measure for a specific crypto-currency network",
    )
    pqp_s.add_argument(
        "--schema",
        default=_DEFAULT_SCHEMA,
        metavar="DB_SCHEMA",
        help="PostgreSQL schema for quality measures tables",
    )
    pqp_s.add_argument(
        "-u", "--url", help="postgresql://user:password@db_host:port/database"
    )
    pqp_s.set_defaults(func=show_quality_measures, url=def_url)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    if hasattr(args, "url") and not args.url:
        print_warn(url_msg)
        parser.error("No postgresql URL connection was provided. Exiting.")

    return args.func(args)


if __name__ == "__main__":
    main()
