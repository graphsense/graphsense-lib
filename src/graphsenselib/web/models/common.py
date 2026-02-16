"""Common API models shared across domains."""

from graphsenselib.web.models.base import APIModel


class LabeledItemRef(APIModel):
    """Reference to a labeled item."""

    id: str
    label: str
