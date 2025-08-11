"""TagPack - A wrappers TagPack Schema"""

import pandas as pd
import yaml

from tagpack import ValidationError
from tagpack.schema import check_type

from .utils import open_pkgresource_file

TAGPACK_SCHEMA_FILE = "tagpack_schema.yaml"
CONFIDENCE_FILE = "confidence.csv"


class TagPackSchema(object):
    """Defines the structure of a TagPack and supports validation"""

    def __init__(self):
        with open_pkgresource_file(TAGPACK_SCHEMA_FILE) as f:
            schema = f.read()  # pkg_resources.read_text(conf, TAGPACK_SCHEMA_FILE)
        self.schema = yaml.safe_load(schema)
        with open_pkgresource_file(CONFIDENCE_FILE) as confidence:
            # confidence = pkg_resources.open_text(db, CONFIDENCE_FILE)
            self.confidences = pd.read_csv(confidence, index_col="id")
        self.definition = TAGPACK_SCHEMA_FILE

    @property
    def header_fields(self):
        return self.schema["header"]

    @property
    def mandatory_header_fields(self):
        return {k: v for k, v in self.schema["header"].items() if v["mandatory"]}

    @property
    def tag_fields(self):
        return self.schema["tag"]

    @property
    def mandatory_tag_fields(self):
        return {k: v for k, v in self.tag_fields.items() if v["mandatory"]}

    @property
    def all_fields(self):
        """Returns all header and body fields"""
        return {**self.header_fields, **self.tag_fields}

    def field_type(self, field):
        return self.all_fields[field]["type"]

    def field_definition(self, field):
        return self.all_fields.get(field, None)

    def field_taxonomy(self, field):
        return self.all_fields[field].get("taxonomy")

    def check_type(self, field, value):
        """Checks whether a field's type matches the definition"""
        # schema_type = self.field_type(field)
        field_def = self.field_definition(field)
        if field_def is None:
            raise ValidationError(f"Field {field} not defined in schema.")
        return check_type(self.schema, field, field_def, value)

    def check_taxonomies(self, field, value, taxonomies):
        """Checks whether a field uses values from given taxonomies, with performance improvements."""
        # Retrieve the taxonomy information once
        taxonomy = self.field_taxonomy(field)
        if not taxonomy:
            return True
        if not taxonomies:
            raise ValidationError("No taxonomies loaded")

        if isinstance(taxonomy, str):
            taxonomy = [taxonomy]

        expected_taxonomies = [taxonomies.get(tid) for tid in taxonomy]
        if None in expected_taxonomies:
            raise ValidationError(f"Unknown taxonomy {taxonomy}")

        valid_concepts = set()
        for t in expected_taxonomies:
            valid_concepts.update(t.concept_ids)

        values = value if isinstance(value, list) else [value]

        # Check each provided value against the union of valid concept IDs
        for v in values:
            if v not in valid_concepts:
                raise ValidationError(f"Undefined concept {v} for {field} field")

        return True
