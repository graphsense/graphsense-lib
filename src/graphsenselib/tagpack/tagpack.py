"""TagPack - A wrapper for TagPacks files"""

# flake8: noqa: T201
import glob
import hashlib
import json
import os
import pathlib
import sys
from collections import UserDict, defaultdict
from datetime import date, datetime
from typing import Tuple

from graphsenselib.config import supported_base_currencies
import giturlparse as gup
import yaml
from git import Repo
from yamlinclude import YamlIncludeConstructor

from graphsenselib.tagpack import (
    TagPackFileError,
    UniqueKeyLoader,
    ValidationError,
    load_yaml_fast,
)
from graphsenselib.tagpack.concept_mapping import map_concepts_to_supported_concepts
from graphsenselib.tagpack.utils import apply_to_dict_field, try_parse_date
from graphsenselib.utils.address import validate_address
from graphsenselib.tagpack.cmd_utils import get_user_choice
import logging

logger = logging.getLogger(__name__)


def get_repository(path: str) -> pathlib.Path:
    """Parse '/home/anna/graphsense/graphsense-tagpacks/public/packs' ->
    and return pathlib object with the repository root
    """
    repo_path = pathlib.Path(path)

    while str(repo_path) != repo_path.root:
        try:
            with Repo(repo_path):
                return repo_path
        except Exception:
            pass
        repo_path = repo_path.parent
    raise ValidationError(f"No repository root found in path {path}")


def get_uri_for_tagpack(
    repo_path, tagpack_file, strict_check, no_git
) -> Tuple[str, str, str, date]:
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

        date_last_mod = datetime.fromtimestamp(os.path.getmtime(tagpack_file))
        return tagpack_file, rel_path, default_prefix, date_last_mod

    with Repo(repo_path) as repo:
        if strict_check and repo.is_dirty():
            msg = f"Local modifications in {repo.common_dir} detected, please "
            msg += "push first."
            logger.info(msg)
            sys.exit(0)

        # Get the list of commits for the specified file
        commits = list(repo.iter_commits(paths=tagpack_file))

        if commits:
            # Get the most recent commit
            latest_commit = commits[0]

            # Convert the commit date (Unix timestamp) to a readable format
            commit_date = datetime.fromtimestamp(latest_commit.committed_date)
        else:
            commit_date = None

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
            tag = next(
                (tag for tag in repo.tags if tag.commit == repo.head.commit), None
            )
            tree_name = tag.name

        res = f"{g}/tree/{tree_name}/{rel_path}"

        default_prefix = hashlib.sha256(g.encode("utf-8")).hexdigest()[:16]

        return res, rel_path, default_prefix, commit_date


def check_for_null_characters(field_name: str, value, context=None) -> None:
    """
    Check if a field value contains null characters (\x00 or \u0000).

    Args:
        field_name: Name of the field being checked
        value: Value to check for null characters
        context: Additional context for error messages (converted to str only on error)

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
        logger.warning(f"Not a valid path: {path}")
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
            logger.warning(msj)

    tagpack_files = {k: v for k, v in tagpack_files.items() if v}

    # exclude files that are too large
    max_bytes = max_mb * 1048576
    for _, files in tagpack_files.items():
        for f in files.copy():
            if os.stat(f).st_size > max_bytes:
                logger.warning(
                    f"{f} is too large and will not be processed: "
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
        self._tags_cache = None

    def _invalidate_cache(self):
        """Invalidate all caches."""
        self._tag_fields_cache = None
        self._tags_cache = None

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

    verifiable_currencies = supported_base_currencies

    @staticmethod
    def _db_unique_key_for_tag(tag):
        """Build the uniqueness key used by tagstore insert/DB constraint.

        Mirrors the normalization applied by tagstore insert to ensure
        validation can detect DB unique constraint violations early.
        """
        # Extract identifier (address or tx_hash)
        if "address" in tag.all_fields:
            address = tag.all_fields.get("address")
            network = str(tag.all_fields.get("network", "")).upper()

            # Apply network-specific address normalization (matches tagstore._perform_address_modifications)
            if network == "BCH" and address.startswith("bitcoincash"):
                from cashaddress.convert import to_legacy_address

                try:
                    address = to_legacy_address(address)
                except Exception as exc:
                    logger.warning(
                        "Could not normalize BCH cash address during validation; "
                        "using original address as-is: %s (%s)",
                        address,
                        exc,
                    )
            elif network == "ETH":
                address = address.lower()

            identifier = address
        else:
            identifier = tag.all_fields.get("tx_hash")

        label = tag.all_fields.get("label")
        label = label.strip() if isinstance(label, str) else ""

        return (
            identifier,
            str(tag.all_fields.get("network", "")).upper(),
            label,
            tag.all_fields.get("source"),
        )

    @staticmethod
    def _shorten_middle(value: str, max_len: int = 50) -> str:
        if len(value) <= max_len:
            return value

        if max_len <= 3:
            return "." * max_len

        keep = max_len - 3
        left = keep // 2
        right = keep - left
        return f"{value[:left]}...{value[-right:]}"

    def _source_prefix(self) -> str:
        """Return a safe source prefix for log messages, or empty string."""
        src_value = ""
        if isinstance(self.uri, (str, os.PathLike)):
            src_value = os.fspath(self.uri)

        if not src_value:
            return ""

        try:
            src = pathlib.Path(src_value).name or src_value
        except Exception:
            src = src_value

        src = self._shorten_middle(str(src), max_len=50)
        return f"[{src}] " if src else ""

    @staticmethod
    def load_from_file(
        uri, pathname, schema, taxonomies, header_dir=None, use_pyyaml=False
    ):
        if not os.path.isfile(pathname):
            sys.exit("This program requires {} to be a file".format(pathname))

        # Check first 4KB for !include directives
        with open(pathname, "rb") as f:
            has_include = b"!include" in f.read(4096)

        if use_pyyaml or header_dir is not None or has_include:
            YamlIncludeConstructor.add_to_loader_class(
                loader_class=UniqueKeyLoader, base_dir=header_dir
            )
            with open(pathname, "r") as f:
                contents = yaml.load(f, UniqueKeyLoader)
        else:
            contents = load_yaml_fast(pathname)

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
            logger.warning(
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
        if self.contents._tags_cache is not None:
            return self.contents._tags_cache
        try:
            self.contents._tags_cache = [
                Tag.from_contents(tag, self) for tag in self.contents["tags"]
            ]
            return self.contents._tags_cache
        except AttributeError:
            raise TagPackFileError("Cannot extract tags from tagpack")

    def get_unique_tags(self):
        if self._unique_tags:
            return self._unique_tags

        seen = set()
        duplicates = []
        self._unique_tags = []

        src = self._source_prefix()

        for tag in self.tags:
            fields = tag.all_fields
            key = (
                str(fields.get("address", fields.get("tx_hash", ""))).lower(),
                str(fields.get("currency", "")).lower(),
                str(fields.get("network", "")).lower(),
                str(fields.get("label", "")).lower(),
                str(fields.get("source", "")).lower(),
            )
            if key in seen:
                # Log warning for duplicate detected
                identifier, _, network, label, source = key
                logger.warning(
                    "%s Duplicate tag will be removed during deduplication on insert: "
                    "label=%s, identifier=%s, network=%s, source=%s",
                    src,
                    label,
                    identifier,
                    network,
                    source,
                )
                duplicates.append(key)
            else:
                seen.add(key)
                self._unique_tags.append(tag)

        self._duplicates = duplicates
        return self._unique_tags

    def validate(self):
        """Validates a TagPack against its schema and used taxonomies"""
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
                logger.warning(
                    "YAML field 'is_public' is DEPRECATED and will be removed "
                    "in future versions. Use the commandline flag "
                    "--public for inserting public tagpacks. By default, tagpacks "
                    "are inserted with access set to private."
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

                check_for_null_characters(field, value, tag)

                # check types and taxomomy use
                try:
                    self.schema.check_type(field, value)
                    self.schema.check_taxonomies(field, value, self.taxonomies)
                except ValidationError as e:
                    raise ValidationError(f"{e} in {tag}")

        if nr_no_actors > 0:
            src_prefix = self._source_prefix()
            logger.warning(
                f"{src_prefix}{nr_no_actors}/{len(ut)} tags have no actor configured. "
                "Please consider connecting the tag to an actor."
            )

        address_counts = defaultdict(int)
        for tag in ut:
            address = tag.all_fields.get("address")
            if address is not None:
                address_counts[address] += 1

        for address, count in address_counts.items():
            if count > 100:
                src_prefix = self._source_prefix()
                logger.warning(
                    f"{src_prefix}{count} tags with the same address {address} found. "
                    "Consider aggregating them."
                )

        # Fail fast if two tags would collide on DB unique constraint after the
        # same normalization used by tagstore insert (_get_network_and_address,
        # label.strip(), network upper-casing).
        unique_tag_index = {}
        for tag in ut:
            key = self._db_unique_key_for_tag(tag)
            if key in unique_tag_index:
                raise ValidationError(
                    "Duplicate tags would violate DB unique constraint "
                    "(identifier, network, label, tagpack, source): "
                    f"{key}"
                )
            unique_tag_index[key] = True

        if self._duplicates:
            src_prefix = self._source_prefix()
            msg = f"{src_prefix}{len(self._duplicates)} duplicate(s) found, starting "
            msg += f"with {self._duplicates[0]}"
            logger.warning(msg)
        return True

    def verify_addresses(self):
        """
        Verify valid blockchain addresses. In
        general, this is done by decoding the address (e.g. to base58) and
        calculating a checksum using the first bytes of the decoded value,
        which should match with the last bytes of the decoded value.
        """

        unsupported = defaultdict(set)
        invalid = defaultdict(list)  # cupper -> [address, ...]
        whitespace_addrs = []
        for tag in self.get_unique_tags():
            currency = tag.all_fields.get("currency", "").lower()
            cupper = currency.upper()
            address = tag.all_fields.get("address")
            if address is not None:
                if len(address) != len(address.strip()):
                    whitespace_addrs.append(address)
                elif currency in self.verifiable_currencies:
                    v = validate_address(currency, address)
                    if not v:
                        invalid[cupper].append(address)
                else:
                    unsupported[cupper].add(address)

        _SAMPLE_SIZE = 3

        def _fmt_summary(items, repr_items=False):
            total = len(items)
            sample = list(items)[:_SAMPLE_SIZE]
            samples = ", ".join(repr(a) if repr_items else a for a in sample)
            extra = f" (+{total - _SAMPLE_SIZE} more)" if total > _SAMPLE_SIZE else ""
            return total, samples, extra

        src_prefix = self._source_prefix()

        if whitespace_addrs:
            total, samples, extra = _fmt_summary(whitespace_addrs, repr_items=True)
            logger.warning(
                f"{src_prefix}{total} address(es) contain whitespace: {samples}{extra}"
            )

        if invalid:
            all_invalid = [a for addrs in invalid.values() for a in addrs]
            currencies = ", ".join(sorted(invalid.keys()))
            total, samples, extra = _fmt_summary(all_invalid)
            logger.warning(
                f"{src_prefix}{total} possible invalid address(es) [{currencies}]: {samples}{extra}"
            )

        if unsupported:
            total_unsupported = sum(len(v) for v in unsupported.values())
            currencies = ", ".join(sorted(unsupported.keys()))
            logger.warning(
                f"{src_prefix}Address verification skipped for {total_unsupported} address(es) "
                f"in unsupported currencies [{currencies}]"
            )

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
                    logger.info(hl_context_str)
                    magic_choice = 1
                    newhl = hl
                    while True:
                        new_candidates = candidates + [
                            (
                                magic_choice,
                                "NOTHING FOUND - Refine Search",
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
            logger.warning("Actor is defined on Tagpack level, skip scanning all tags.")
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
            logger.warning("Did not assign an actor to the tags with labels:")
            for hl in labels_with_no_actors:
                logger.warning(f" - {hl}")
            logger.warning("Consider creating a suitable actor or manual linking.")

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

        # the yaml parser does not deal with string quoted dates.
        # so '2022-10-1' is not interpreted as a date. This line fixes this.
        apply_to_dict_field(self.contents, "lastmod", try_parse_date, fail=False)

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
