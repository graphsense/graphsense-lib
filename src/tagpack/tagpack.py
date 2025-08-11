"""TagPack - A wrapper for TagPacks files"""

import glob
import hashlib
import json
import os
import pathlib
import sys
from collections import UserDict, defaultdict
from datetime import date

import coinaddrvalidator
import giturlparse as gup
import yaml
from git import Repo
from yamlinclude import YamlIncludeConstructor

from tagpack import TagPackFileError, UniqueKeyLoader, ValidationError
from tagpack.cmd_utils import bcolors, get_user_choice, print_info, print_warn
from tagpack.concept_mapping import map_concepts_to_supported_concepts
from tagpack.constants import (
    is_known_currency,
    is_known_network,
    suggest_networks_from_currency,
)
from tagpack.utils import apply_to_dict_field, try_parse_date


class InconsistencyChecker:
    def __init__(self):
        self.currency_seen = dict()
        self.network_seen = dict()

    def warn_on_possibly_inconsistent_currency_or_network(self, field, value):
        if (
            field == "currency"
            and not self.currency_seen.get(value, False)
            and not is_known_currency(value)
        ):
            print_warn(
                (
                    f"{value} is not a known currency. "
                    "Be careful to avoid introducing ambiguity into the dataset."
                )
            )
            self.currency_seen[value] = True

        if (
            field == "network"
            and not self.network_seen.get(value, False)
            and not is_known_network(value)
        ):
            suggestions = suggest_networks_from_currency(value)
            print_warn(
                (
                    (
                        f"{value} is not a known network. "
                        "Be careful to avoid introducing ambiguity into the dataset. "
                    )
                    + (
                        f"Did you mean on of: {', '.join(suggestions)}"
                        if len(suggestions) > 0
                        else ""
                    )
                )
            )
            self.network_seen[value] = True


def get_repository(path: str) -> pathlib.Path:
    """Parse '/home/anna/graphsense/graphsense-tagpacks/public/packs' ->
    and return pathlib object with the repository root
    """
    repo_path = pathlib.Path(path)

    while str(repo_path) != repo_path.root:
        try:
            Repo(repo_path)
            return repo_path
        except Exception:
            pass
        repo_path = repo_path.parent
    raise ValidationError(f"No repository root found in path {path}")


def get_uri_for_tagpack(repo_path, tagpack_file, strict_check, no_git):
    """For a given path string
        '/home/anna/graphsense/graphsense-tagpacks/public/packs'

    and tagpack file string
        '/home/anna/graphsense/graphsense-tagpacks/public/packs/a/2021/01/2010101.yaml'

    return remote URI
        'https://github.com/anna/tagpacks/blob/develop/public/packs/a/2021/01/2010101.yaml'

        and relative path
        'a/2021/01/2010101.yaml'

    If no_git is set, return tagpack file string. For the relative path,
    try to split at '/packs/', or as fallback return the absolute path.

    Local git copy will be checked for modifications by default.
    Toggle strict_check param to change this.

    If path does not contain any git information, the original path
    is returned.
    """
    default_prefix = hashlib.sha256("".encode("utf-8")).hexdigest()[:16]
    if no_git:
        if "/packs/" in tagpack_file:
            rel_path = tagpack_file.split("/packs/")[1]

        else:
            rel_path = tagpack_file
        return tagpack_file, rel_path, default_prefix

    repo = Repo(repo_path)

    if strict_check and repo.is_dirty():
        msg = f"Local modifications in {repo.common_dir} detected, please "
        msg += "push first."
        print_info(msg)
        sys.exit(0)

    if len(repo.remotes) > 1:
        msg = (
            f"Multiple remotes present, cannot "
            f"decide on backlink. Remotes: {repo.remotes}"
        )
        raise ValidationError(msg)

    rel_path = str(pathlib.Path(tagpack_file).relative_to(repo_path))

    u = next(repo.remotes[0].urls)
    if u.endswith("/"):
        u = u[:-1]
    if not u.endswith(".git"):
        u += ".git"

    g = gup.parse(u).url2https.replace(".git", "")

    try:
        tree_name = repo.active_branch.name
    except TypeError:
        # needed if a tags is checked out eg. in ci
        # tree_name = repo.git.describe()
        tag = next((tag for tag in repo.tags if tag.commit == repo.head.commit), None)
        tree_name = tag.name

    res = f"{g}/tree/{tree_name}/{rel_path}"

    default_prefix = hashlib.sha256(g.encode("utf-8")).hexdigest()[:16]

    return res, rel_path, default_prefix


def check_for_null_characters(field_name: str, value, context: str = "") -> None:
    """
    Check if a field value contains null characters (\x00 or \u0000).

    Args:
        field_name: Name of the field being checked
        value: Value to check for null characters
        context: Additional context for error messages (e.g., tag info)

    Raises:
        ValidationError: If null characters are found in the value
    """
    if isinstance(value, str):
        if "\x00" in value or "\u0000" in value:
            context_str = f" in {context}" if context else ""
            raise ValidationError(
                f"Field '{field_name}' contains null characters (\\x00 or \\u0000){context_str}"
            )
    elif isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            if isinstance(item, str) and ("\x00" in item or "\u0000" in item):
                context_str = f" in {context}" if context else ""
                raise ValidationError(
                    f"Field '{field_name}' item at index {i} contains null characters (\\x00 or \\u0000){context_str}"
                )


def collect_tagpack_files(path, search_actorpacks=False, max_mb=200):
    """
    Collect Tagpack YAML files from the given path. This function returns a
    dict made of sets. Each key of the dict is the corresponding header path of
    the values included in its set (the one in the closest parent directory).
    The None value is the key for files without header path. By convention, the
    name of a header file should be header.yaml
    """
    tagpack_files = {}

    if os.path.isdir(path):
        files = set(glob.glob(path + "/**/*.yaml", recursive=True))
    elif os.path.isfile(path):  # validate single file
        files = {path}
    else:  # TODO Error! Should we validate the path within __main__?
        print_warn(f"Not a valid path: {path}")
        return {}

    files = {f for f in files if not f.endswith("config.yaml")}

    if search_actorpacks:
        files = {f for f in files if f.endswith("actorpack.yaml")}
    else:
        files = {f for f in files if not f.endswith("actorpack.yaml")}

    sfiles = sorted(files, key=lambda x: (-len(x.split(os.sep)), x))
    # Select headers
    hfiles = [f for f in sfiles if f.endswith("header.yaml")]
    # Remove header files from the search
    files -= set(hfiles)
    # Map files and headers
    for f in hfiles:
        header = os.path.dirname(f)
        # Select files in the same path than header, subdirs only
        match_files = {
            mfile
            for mfile in files
            if (header in mfile and len(mfile.split(os.sep)) > len(f.split(os.sep)))
        }
        tagpack_files[header] = match_files
        files -= match_files

    # Files without headers
    if files:
        files_ = sorted(files, key=lambda x: (-len(x.split(os.sep)), x))
        tagpack_files[None] = files_

    # Avoid to include header files without files
    for t, fs in tagpack_files.items():
        if not fs:
            msj = f"\tThe header file in {os.path.realpath(t)} won't be "
            msj += "included in any tagpack"
            print_warn(msj)

    tagpack_files = {k: v for k, v in tagpack_files.items() if v}

    # exclude files that are too large
    max_bytes = max_mb * 1048576
    for _, files in tagpack_files.items():
        for f in files.copy():
            if os.stat(f).st_size > max_bytes:
                print_warn(
                    f"{f} is too large and will be not be processed: "
                    f"{(os.stat(f).st_size / 1048576):.2f} mb, current "
                    f"max file size is {max_mb} mb. "
                    "Please split the file to be processed."
                )
                files.remove(f)

    return tagpack_files


class TagPackContents(UserDict):
    def __init__(self, contents, schema):
        super().__init__(contents)
        self.schema = schema
        self._tag_fields_cache = None

    def _invalidate_cache(self):
        """Invalidate the cached tag_fields."""
        self._tag_fields_cache = None

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._invalidate_cache()

    def __delitem__(self, key):
        super().__delitem__(key)
        self._invalidate_cache()

    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)
        self._invalidate_cache()

    def clear(self):
        super().clear()
        self._invalidate_cache()

    def rebuild_cache(self):
        self._tag_fields_cache = {
            k: v for k, v in self.data.items() if k in self.schema.tag_fields
        }

    @property
    def tag_fields(self):
        """
        Return a cached dictionary of items where keys are in the schema's tag_fields.
        The cache is invalidated whenever the dictionary is modified.
        """
        if self._tag_fields_cache is None:
            self.rebuild_cache()
        return self._tag_fields_cache


class TagPack(object):
    """Represents a TagPack"""

    def __init__(self, uri, contents, schema, taxonomies):
        self.uri = uri
        self.contents = TagPackContents(contents, schema)
        self.schema = schema
        self.taxonomies = taxonomies
        self._unique_tags = []
        self._duplicates = []
        self.tag_fields_dict = None

        self.init_default_values()

        # the yaml parser does not deal with string quoted dates.
        # so '2022-10-1' is not interpreted as a date. This line fixes this.
        apply_to_dict_field(self.contents, "lastmod", try_parse_date, fail=False)

    verifiable_currencies = [
        a.ticker for a in coinaddrvalidator.currency.Currencies.instances.values()
    ]

    def load_from_file(uri, pathname, schema, taxonomies, header_dir=None):
        YamlIncludeConstructor.add_to_loader_class(
            loader_class=UniqueKeyLoader, base_dir=header_dir
        )

        if not os.path.isfile(pathname):
            sys.exit("This program requires {} to be a file".format(pathname))
        contents = yaml.load(open(pathname, "r"), UniqueKeyLoader)

        if "header" in contents.keys():
            for k, v in contents["header"].items():
                contents[k] = v
            contents.pop("header")
        return TagPack(uri, contents, schema, taxonomies)

    def update_lastmod(self):
        self.contents["lastmod"] = date.today()

    def init_default_values(self):
        if "confidence" not in self.contents and not all(
            "confidence" in tag.contents for tag in self.tags
        ):
            conf_scores_df = self.schema.confidences
            min_confs = conf_scores_df[
                conf_scores_df.level == conf_scores_df.level.min()
            ]
            lowest_confidence_score = (
                min_confs.index[-1] if len(min_confs) > 0 else None
            )
            self.contents["confidence"] = lowest_confidence_score
            print_warn(
                "Not all tags have a confidence score set. "
                f"Set default confidence level to {lowest_confidence_score} "
                "on tagpack level."
            )

        # if network is not provided in the file, set it to the currency
        # warnings will be issued in the validate step.
        if "network" not in self.contents and "currency" in self.contents:
            self.contents["network"] = self.contents["currency"]

        for t in self.tags:
            if "network" not in t.contents and "currency" in t.contents:
                t.contents["network"] = t.contents["currency"]

    @property
    def all_header_fields(self):
        """Returns all TagPack header fields, including generic tag fields"""
        try:
            return {k: v for k, v in self.contents.items()}  # noqa: C416
        except AttributeError:
            raise TagPackFileError("Cannot extract TagPack fields")

    @property
    def header_fields(self):
        """Returns only TagPack header fields that are defined as such"""
        try:
            return {
                k: v for k, v in self.contents.items() if k in self.schema.header_fields
            }
        except AttributeError:
            raise TagPackFileError("Cannot extract TagPack fields")

    @property
    def tag_fields(self):
        """Returns tag fields defined in the TagPack header"""
        try:
            return self.contents.tag_fields
        except AttributeError:
            raise TagPackFileError("Cannot extract TagPack fields")

    @property
    def tags(self):
        """Returns all tags defined in a TagPack's body"""
        try:
            return [Tag.from_contents(tag, self) for tag in self.contents["tags"]]
        except AttributeError:
            raise TagPackFileError("Cannot extract tags from tagpack")

    def get_unique_tags(self):
        if self._unique_tags:
            return self._unique_tags

        keys = ("address", "currency", "network", "label", "source")
        seen = set()
        duplicates = []
        self._unique_tags = []

        for tag in self.tags:
            fields = tag.all_fields
            key_tuple = tuple(str(fields.get(k, "")).lower() for k in keys)
            if key_tuple in seen:
                duplicates.append(key_tuple)
            else:
                seen.add(key_tuple)
                self._unique_tags.append(tag)

        self._duplicates = duplicates
        return self._unique_tags

    def validate(self):
        """Validates a TagPack against its schema and used taxonomies"""
        inconsistency_checker = InconsistencyChecker()
        # check if mandatory header fields are used by a TagPack
        for schema_field in self.schema.mandatory_header_fields:
            if schema_field not in self.header_fields:
                raise ValidationError(
                    "Mandatory header field {} missing".format(schema_field)
                )

        # check header fields' types, taxonomy and mandatory use
        for field, value in self.all_header_fields.items():
            # check a field is defined
            if field not in self.schema.all_fields:
                raise ValidationError("Field {} not allowed in header".format(field))
            # check for None values
            if value is None:
                raise ValidationError(
                    "Value of header field {} must not be empty (None)".format(field)
                )

            check_for_null_characters(field, value, "header")

            if field == "is_public":
                print_warn(
                    "YAML field 'is_public' is DEPRECATED and will be removed "
                    "in future versions. Use the commandline flag "
                    "--public for inserting public tagpacks. By default, tagpacks "
                    "are inserted with access set to private."
                )

            inconsistency_checker.warn_on_possibly_inconsistent_currency_or_network(
                field, value
            )

            self.schema.check_type(field, value)
            self.schema.check_taxonomies(field, value, self.taxonomies)

        # iterate over all tags, check types, taxonomy and mandatory use
        e2 = "Mandatory tag field {} missing in {}"
        e3 = "Field {} not allowed in {}"
        e4 = "Value of body field {} must not be empty (None) in {}"

        ut = self.get_unique_tags()
        nr_no_actors = 0
        for tag in ut:
            # check if mandatory tag fields are defined
            if not isinstance(tag, Tag):
                raise ValidationError("Unknown tag type {}".format(tag))

            actor = tag.all_fields.get("actor", None)
            if actor is None:
                nr_no_actors += 1

            address = tag.all_fields.get("address", None)
            tx_hash = tag.all_fields.get("tx_hash", None)
            if address is None and tx_hash is None:
                raise ValidationError(e2.format("address", tag))
            elif address is not None and tx_hash is not None:
                raise ValidationError(
                    "The fields tx_hash and address are mutually exclusive but both are set."
                )

            for schema_field in self.schema.mandatory_tag_fields:
                if (
                    schema_field not in tag.explicit_fields
                    and schema_field not in self.tag_fields
                ):
                    raise ValidationError(e2.format(schema_field, tag))

            for field, value in tag.explicit_fields.items():
                # check whether field is defined as body field
                if field not in self.schema.tag_fields:
                    raise ValidationError(e3.format(field, tag))

                # check for None values
                if value is None:
                    raise ValidationError(e4.format(field, tag))

                check_for_null_characters(field, value, str(tag))

                inconsistency_checker.warn_on_possibly_inconsistent_currency_or_network(
                    field, value
                )

                # check types and taxomomy use
                try:
                    self.schema.check_type(field, value)
                    self.schema.check_taxonomies(field, value, self.taxonomies)
                except ValidationError as e:
                    raise ValidationError(f"{e} in {tag}")

        if nr_no_actors > 0:
            print_warn(
                f"{nr_no_actors}/{len(ut)} tags have no actor configured. "
                "Please consider connecting the tag to an actor."
            )

        address_counts = defaultdict(int)
        for tag in ut:
            address = tag.all_fields.get("address")
            if address is not None:
                address_counts[address] += 1

        for address, count in address_counts.items():
            if count > 100:
                print_warn(
                    f"{count} tags with the same address {address} found. "
                    "Consider aggregating them."
                )

        if self._duplicates:
            msg = f"{len(self._duplicates)} duplicate(s) found, starting "
            msg += f"with {self._duplicates[0]}\n"
            print_info(msg)
        return True

    def verify_addresses(self):
        """
        Verify valid blockchain addresses using coinaddrvalidator library. In
        general, this is done by decoding the address (e.g. to base58) and
        calculating a checksum using the first bytes of the decoded value,
        which should match with the last bytes of the decoded value.
        """

        unsupported = defaultdict(set)
        msg = "Possible invalid {} address: {}"
        for tag in self.get_unique_tags():
            currency = tag.all_fields.get("currency", "").lower()
            cupper = currency.upper()
            address = tag.all_fields.get("address")
            if address is not None:
                if len(address) != len(address.strip()):
                    print_warn(f"Address contains whitespace: {repr(address)}")
                elif currency in self.verifiable_currencies:
                    v = coinaddrvalidator.validate(currency, address)
                    if not v.valid:
                        print_warn(msg.format(cupper, address))
                else:
                    unsupported[cupper].add(address)

        for c, addrs in unsupported.items():
            print_warn(f"Address verification is not supported for {c}:")
            for a in sorted(addrs):
                print_warn(f"\t{a}")

    def add_actors(
        self, find_actor_candidates, only_categories=None, user_choice_cache={}
    ) -> bool:
        """Suggest actors for labels that have no actors assigned

        Args:
            find_actor_candidates (Function): function taking a label
            returning a list of actor candidates, either as list[str]
            or as a list[tuple[str,str]] where the first entry is a id
            and the second a human readable label of the entry.

            only_categories (None, optional): List of tag-categories to edit.


        Returns:
            bool: true if suggestions where found
        """

        suggestions_found = False
        labels_with_no_actors = set()

        def get_user_choice_cached(hl, hl_context_str, cache):
            # normalize label to allow for better matching
            hl = hl.replace("_", " ").replace("-", " ").replace(".", " ").lower()
            if hl in cache:
                return cache[hl]
            else:
                candidates = find_actor_candidates(hl)
                if len(candidates) == 0:
                    choice = None
                else:
                    print(hl_context_str)
                    magic_choice = 1
                    newhl = hl
                    while True:
                        new_candidates = candidates + [
                            (
                                magic_choice,
                                f"{bcolors.BOLD}NOTHING FOUND - Refine Search",
                            )
                        ]
                        choice = get_user_choice(newhl, new_candidates)
                        if choice == magic_choice:
                            newhl = input("New search term: ")
                            candidates = find_actor_candidates(newhl)
                        else:
                            break

                cache[hl] = choice
                return choice

        if (
            "label" in self.all_header_fields
            and "actor" not in self.all_header_fields
            and (
                only_categories is None
                or self.all_header_fields.get("category", "") in only_categories
            )
        ):
            hl = self.all_header_fields.get("label")
            # candidates = find_actor_candidates(hl)
            actor = get_user_choice_cached(hl, "", user_choice_cache)

            if actor:
                self.contents["actor"] = actor
                suggestions_found = True
            else:
                labels_with_no_actors.add(hl)

        if "actor" in self.all_header_fields and not suggestions_found:
            print_warn("Actor is defined on Tagpack level, skip scanning all tags.")
            return False

        # update tags and trace if all labels carry the same actor
        actors = set()
        all_tags_carry_actor = True
        for tag in self.get_unique_tags():
            # Continue if tag is not of a selected category
            if (
                only_categories is not None
                and tag.all_fields.get("category") not in only_categories
            ):
                continue

            if "label" in tag.explicit_fields and "actor" not in tag.explicit_fields:
                tl = tag.explicit_fields.get("label")
                context_str = f"Working on tag: \n{tag}\n"
                actor = get_user_choice_cached(tl, context_str, user_choice_cache)
                if actor:
                    tag.contents["actor"] = actor
                    actors.add(actor)
                    suggestions_found = True
                else:
                    labels_with_no_actors.add(tl)
                    all_tags_carry_actor = False

        if all_tags_carry_actor and len(actors) == 1:
            # promote actor to header field
            self.contents["actor"] = actors.pop()
            for tag in self.get_unique_tags():
                tag.contents.pop("actor")

        if len(labels_with_no_actors) > 0:
            print_warn("Did not assign an actor to the tags with labels:")
            for hl in labels_with_no_actors:
                print_warn(f" - {hl}")
            print_warn("Consider creating a suitable actor or manual linking.")

        return suggestions_found

    def to_json(self):
        """Returns a JSON representation of a TagPack's header"""
        tagpack = {}
        tagpack["uri"] = self.uri
        for k, v in self.header_fields.items():
            if k != "tags":
                tagpack[k] = v
        return json.dumps(tagpack, indent=4, sort_keys=True, default=str)

    def __str__(self):
        """Returns a string serialization of the entire TagPack"""
        return str(self.contents)


class Tag(object):
    """An attribution tag"""

    def __init__(self, contents, tagpack):
        self.contents = contents
        self.tagpack = tagpack

        # This allows the context in the yaml file to be written in eithe
        # normal yaml syntax which is now converted to a json string
        # of directly as json string.
        if isinstance(self.contents.get("context", None), dict):
            apply_to_dict_field(self.contents, "context", json.dumps, fail=True)

        # set default values for concepts field
        # make sure abuse and category are always part of the context
        concepts = self.all_fields.get("concepts", [])
        category = self.all_fields.get("category", None)
        abuse = self.all_fields.get("abuse", None)
        if abuse and abuse not in concepts:
            concepts.append(abuse)

        if category and category not in concepts:
            concepts.append(category)

        # add tags from "tags" field in concepts.
        try:
            ctx = self.all_fields.get("context")
            if ctx is not None:
                tags = json.loads(ctx).get("tags", None)
                if tags is not None:
                    mcs = map_concepts_to_supported_concepts(tags)
                    for mc in mcs:
                        if mc not in concepts:
                            concepts.append(mc)
        except json.decoder.JSONDecodeError:
            pass

        self.contents["concepts"] = concepts

    @staticmethod
    def from_contents(contents, tagpack):
        return Tag(contents, tagpack)

    @property
    def explicit_fields(self):
        """Return only explicitly defined tag fields"""
        return self.contents  # noqa: C416

    @property
    def all_fields(self):
        """Return all tag fields (explicit and generic)"""
        return {
            **self.tagpack.tag_fields,
            **self.explicit_fields,
        }

    def to_json(self):
        """Returns a JSON serialization of all tag fields"""
        tag = self.all_fields
        tag["tagpack_uri"] = self.tagpack.uri
        return json.dumps(tag, indent=4, sort_keys=True, default=str)

    def __str__(self):
        """ "Returns a string serialization of a Tag"""
        return "\n".join([f"{k}={v}" for k, v in self.all_fields.items()])
