"""ActorPack - A wrapper for ActorPack files"""

import json
import os
import re
import sys
from collections import defaultdict
from typing import Optional

import yaml
from yamlinclude import YamlIncludeConstructor

from tagpack import TagPackFileError, UniqueKeyLoader, ValidationError
from tagpack.cmd_utils import print_warn
from tagpack.utils import (
    apply_to_dict_field,
    get_secondlevel_domain,
    normalize_id,
    strip_empty,
    try_parse_date,
)

LBL_BLACKLIST = re.compile(r"[@_!#$%^*<>?\|}{~:;]")


class ActorPack(object):
    """Represents an ActorPack"""

    def __init__(self, uri, contents, schema, taxonomies):
        self.uri = uri
        self.contents = contents
        self.schema = schema
        self.taxonomies = taxonomies
        self._unique_actors = []
        self._duplicates = []
        self._resolve_mapping = {}

        # the yaml parser does not deal with string quoted dates.
        # so '2022-10-1' is not interpreted as a date. This line fixes this.
        apply_to_dict_field(self.contents, "lastmod", try_parse_date, fail=False)

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
        return ActorPack(uri, contents, schema, taxonomies)

    @property
    def all_header_fields(self):
        """Returns all ActorPack header fields, including generic actor fields"""
        try:
            return {k: v for k, v in self.contents.items()}  # noqa: C416
        except AttributeError:
            raise TagPackFileError("Cannot extract ActorPack fields")

    @property
    def header_fields(self):
        """Returns only ActorPack header fields that are defined as such"""
        try:
            return {
                k: v for k, v in self.contents.items() if k in self.schema.header_fields
            }
        except AttributeError:
            raise TagPackFileError("Cannot extract ActorPack fields")

    @property
    def actor_fields(self):
        """Returns actor fields defined in the ActorPack header"""
        try:
            return {
                k: v
                for k, v in self.contents.items()
                if k != "actors" and k in self.schema.actor_fields
            }
        except AttributeError:
            raise TagPackFileError("Cannot extract ActorPack fields")

    @property
    def actors(self):
        """Returns all actors defined in a ActorPack's body"""
        try:
            return [
                Actor.from_contents(actor, self) for actor in self.contents["actors"]
            ]
        except AttributeError:
            raise TagPackFileError("Cannot extract actors from ActorPack")

    def get_unique_actors(self):
        if self._unique_actors:
            return self._unique_actors

        seen = set()
        duplicates = []

        for actor in self.actors:
            # check if duplicate entry
            t = tuple(str(actor.all_fields.get(k)).lower() for k in ["id", "label"])
            if t in seen:
                duplicates.append(t)
            else:
                seen.add(t)
                self._unique_actors.append(actor)

        self._duplicates = duplicates
        return self._unique_actors

    def get_resolve_mapping(self):
        """Returns a mapping of aliases to actor ids"""
        if self._resolve_mapping:
            return self._resolve_mapping

        unique_actors = self.get_unique_actors()

        mapping = {actor.identifier: actor.identifier for actor in unique_actors}

        for actor in unique_actors:
            for alias in actor.all_fields.get("aliases", []):
                mapping[alias] = actor.identifier

        self._resolve_mapping = mapping
        return mapping

    def resolve_actor(self, identifier) -> Optional[str]:
        """Uses id and alias to map a given identifier to an actor id"""
        mapping = self.get_resolve_mapping()

        return mapping.get(identifier, None)

    def validate(self):
        """Validates an ActorPack against its schema and used taxonomies"""

        # check if mandatory header fields are used by an ActorPack
        for schema_field in self.schema.mandatory_header_fields:
            if schema_field not in self.header_fields:
                msg = f"Mandatory header field {schema_field} missing"
                raise ValidationError(msg)

        # check header fields' types, taxonomy and mandatory use
        for field, value in self.all_header_fields.items():
            # check a field is defined
            if field not in self.schema.all_fields:
                raise ValidationError(f"Field {field} not allowed in header")
            # check for None values
            if value is None:
                msg = f"Value of header field {field} must not be empty (None)"
                raise ValidationError(msg)

            self.schema.check_type(field, value)
            self.schema.check_taxonomies(field, value, self.taxonomies)

        if len(self.actors) < 1:
            raise ValidationError("No actors found.")

        # iterate over all tags, check types, taxonomy and mandatory use
        e2 = "Mandatory tag field {} missing in {}"
        e3 = "Field {} not allowed in {}"
        e4 = "Value of body field {} must not be empty (None) in {}"
        domain_overlap = defaultdict(set)
        twitter_handle_overlap = defaultdict(set)
        github_organisation_overlap = defaultdict(set)
        ids = defaultdict(int)
        for actor in self.get_unique_actors():
            # check if mandatory actor fields are defined
            if not isinstance(actor, Actor):
                raise ValidationError(f"Unknown actor type {type(actor)}")

            ids[actor.identifier] += 1

            for schema_field in self.schema.mandatory_actor_fields:
                if (
                    schema_field not in actor.explicit_fields
                    and schema_field not in self.actor_fields
                ):
                    raise ValidationError(e2.format(schema_field, actor))

            for field, value in actor.explicit_fields.items():
                # check whether field is defined as body field
                if field not in self.schema.actor_fields:
                    raise ValidationError(e3.format(field, actor))

                # check for None values
                if value is None:
                    raise ValidationError(e4.format(field, actor))

                # check types and taxomomy use
                try:
                    self.schema.check_type(field, value)
                    self.schema.check_taxonomies(field, value, self.taxonomies)
                except ValidationError as e:
                    raise ValidationError(f"{e} in {actor}")

            duplicated_ids = [i for i, c in ids.items() if c > 1]
            if any(duplicated_ids):
                raise ValidationError(
                    f"Actor ids used more than once: {duplicated_ids}"
                )

            lbl = actor.all_fields["label"]
            if LBL_BLACKLIST.search(lbl):
                print_warn(
                    f"Actor {actor.identifier}: label {lbl} contains special "
                    "characters. Please avoid."
                )

            for uri in set(actor.uris):
                if "." not in uri:
                    print_warn(
                        f"There is no dot in uri: {uri} in actor {actor.identifier}"
                    )
                domain_overlap[get_secondlevel_domain(uri)].add(actor.identifier)

            if actor.twitter_handle:
                for handle in actor.twitter_handle.split(","):
                    twitter_handle_overlap[handle.strip().lower()].add(actor.identifier)

            if actor.github_organisation:
                for handle in actor.github_organisation.split(","):
                    github_organisation_overlap[handle.strip().lower()].add(
                        actor.identifier
                    )

        unique_actors = self.get_unique_actors()
        missing_mappings = []
        global_aliases = []
        for actor in unique_actors:
            normalized_id = normalize_id(actor.identifier)
            aliases = actor.all_fields.get("aliases", [])
            global_aliases.extend(aliases)

            if normalized_id != actor.identifier and normalized_id not in aliases:
                missing_mappings.append((actor.identifier, normalized_id))

        if missing_mappings:
            error_message = (
                "For the following actor ids, the normalized id must either replace the original id "
                "or be added to the aliases: \n"
                + "\n".join(
                    f"'{orig}' normalized to '{norm}'"
                    for orig, norm in missing_mappings
                )
            )
            raise ValidationError(error_message)

        actor_ids = {actor.identifier for actor in unique_actors}
        collision = actor_ids & set(global_aliases)
        if collision:
            raise ValidationError(
                f"Collision detected: Actor ids and aliases share the following values: {collision}"
            )

        for domain, actors in domain_overlap.items():
            if len(actors) > 1:
                print_warn(
                    f"Actors share the same domain {domain}: {actors}. Please merge!"
                )

        for twitter_handle, actors in twitter_handle_overlap.items():
            if len(actors) > 1:
                print_warn(
                    "These actors share the same twitter_handle "
                    f" {twitter_handle}: {actors}. Consider Merge?"
                )

        if self._duplicates:
            msg = (
                f"{len(self._duplicates)} duplicate(s) found, starting "
                f"with {self._duplicates[0]}\n"
            )
            raise ValidationError(msg)
        return True

    def to_json(self):
        """Returns a JSON representation of an ActorPack's header"""
        actorpack = {}
        for k, v in self.header_fields.items():
            if k != "actors":
                actorpack[k] = v
        return json.dumps(actorpack, indent=4, sort_keys=True, default=str)

    def __str__(self):
        """Returns a string serialization of the entire ActorPack"""
        return str(self.contents)


class Actor(object):
    """An actor"""

    def __init__(self, contents, actorpack):
        self.contents = contents
        self.actorpack = actorpack

        # This allows the context in the yaml file to be written in either
        # normal yaml syntax which is now converted to a json string
        # of directly as json string.
        if isinstance(self.contents.get("context", None), dict):
            apply_to_dict_field(self.contents, "context", json.dumps, fail=True)

    @staticmethod
    def from_contents(contents, actorpack):
        return Actor(contents, actorpack)

    @property
    def explicit_fields(self):
        """Return only explicitly defined actor fields"""
        return {k: v for k, v in self.contents.items()}  # noqa: C416

    @property
    def all_fields(self):
        """Return all actor fields (explicit and generic)"""
        return {
            **self.actorpack.actor_fields,
            **self.explicit_fields,
        }

    @property
    def context(self):
        if "context" in self.contents and self.contents["context"]:
            return json.loads(self.contents["context"])
        else:
            return {}

    @property
    def uris(self):
        c = self.context
        return strip_empty([self.contents.get("uri", None)] + c.get("uris", []))

    @property
    def twitter_handle(self):
        return self.context.get("twitter_handle", None)

    @property
    def github_organisation(self):
        return self.context.get("github_organisation", None)

    @property
    def identifier(self):
        return self.contents.get("id", None)

    def to_json(self):
        """Returns a JSON serialization of all actor fields"""
        actor = self.all_fields
        return json.dumps(actor, indent=4, sort_keys=True, default=str)

    def __str__(self):
        """ "Returns a string serialization of an Actor"""
        return str(self.all_fields)
