"""Confidence - A proxy for a local confidence definition"""

import csv
import json

from .utils import open_localfile_with_pkgresource_fallback


class Score(object):
    """Score Definition.
    This class serves as a proxy for a score that is locally defined.
    """

    def __init__(self, id, label, description, level):
        self.id = id
        self.label = label
        self.description = description
        self.level = level

    def to_json(self):
        return json.dumps(
            {
                "id": self.id,
                "label": self.label,
                "description": self.description,
                "level": self.level,
            }
        )

    def __str__(self):
        return f"[{self.id}|{self.label}|{self.description}|{self.level}]"


class Confidence(object):
    """TagPack Confidence Proxy.
    This class serves as a proxy for locally defined confidence scores.
    """

    def __init__(self, path):
        self.path = path
        self.scores = []

    def load_from_local(self):
        with open_localfile_with_pkgresource_fallback(self.path) as f:
            csv_reader = csv.DictReader(f, delimiter=",")
            for row in csv_reader:
                score = Score(row["id"], row["label"], row["description"], row["level"])
                self.scores.append(score)

    @property
    def score_ids(self):
        return [score.id for score in self.scores]

    def to_json(self):
        return json.dumps(
            {
                "path": self.path,
                "scores": [json.loads(score.to_json()) for score in self.scores],
            }
        )

    def __str__(self):
        return f"[{self.path}|n_scores:{len(self.score_ids)}]"
