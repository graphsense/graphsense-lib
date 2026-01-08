"""Tests for concept_mapping module."""

from graphsenselib.tagpack.concept_mapping import (
    map_concept_to_supported_concepts,
    map_concepts_to_supported_concepts,
)


class TestMapConceptToSupportedConcepts:
    """Tests for map_concept_to_supported_concepts function."""

    def test_string_concept_mapping(self):
        """Test basic concept mapping, case insensitivity, and whitespace."""
        # Direct mapping
        assert map_concept_to_supported_concepts("market") == {"market"}
        assert map_concept_to_supported_concepts("exchange") == {"exchange"}

        # Case insensitive
        assert map_concept_to_supported_concepts("MARKET") == {"market"}

        # Whitespace handling
        assert map_concept_to_supported_concepts("  market  ") == {"market"}
        assert map_concept_to_supported_concepts("financial crime") == {
            "financial_crime"
        }

        # Unknown concepts return empty set
        assert map_concept_to_supported_concepts("unknown_concept") == set()

    def test_regex_based_mapping(self):
        """Test regex-based concept mappings (re.match from start of string)."""
        assert "drugs" in map_concept_to_supported_concepts("cannabis")
        assert "weapons" in map_concept_to_supported_concepts("firearms")
        assert "hosting" in map_concept_to_supported_concepts("hosting service")


class TestMapConceptsToSupportedConcepts:
    """Tests for map_concepts_to_supported_concepts function (plural)."""

    def test_multiple_concepts(self):
        """Test mapping multiple concepts with filtering of unknown ones."""
        result = map_concepts_to_supported_concepts(
            ["market", "unknown_thing", "exchange"]
        )
        assert result == {"market", "exchange"}

        # Empty input
        assert map_concepts_to_supported_concepts([]) == set()
