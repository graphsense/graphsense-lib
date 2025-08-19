import pytest

pytest.importorskip("yamlinclude", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack.cli import _load_taxonomies

TAXONOMY_URL = "https://graphsense.github.io"


@pytest.fixture
def online_tax_config():
    return {
        "taxonomies": {
            "entity": f"{TAXONOMY_URL}/DW-VA-Taxonomy/assets/data/entities.csv",
            "abuse": f"{TAXONOMY_URL}/DW-VA-Taxonomy/assets/data/abuses.csv",
            "confidence": "src/tagpack/db/confidence.csv",
            "country": "src/tagpack/db/countries.csv",
        }
    }


@pytest.fixture
def all_offline_tax_config():
    return {
        "taxonomies": {
            "confidence": "src/tagpack/db/confidence.csv",
            "country": "src/tagpack/db/countries.csv",
        }
    }


def test_load_online_taxonomies_csv(online_tax_config):
    data = _load_taxonomies(online_tax_config)

    assert len(data["entity"].concept_ids) == 34
    assert len(data["abuse"].concept_ids) == 13
    assert len(data["confidence"].concept_ids) == 14
    assert len(data["country"].concept_ids) == 249


def test_load_offline_taxonomies_yaml(all_offline_tax_config):
    data = _load_taxonomies(all_offline_tax_config)

    # assert len(data["entity"].concept_ids) == 35
    # assert len(data["abuse"].concept_ids) == 13
    assert len(data["confidence"].concept_ids) == 14
    assert len(data["country"].concept_ids) == 249


def test_mockargs_are_pickelable():
    """Test that mockargs are picklable."""
    from graphsenselib.tagpack.cli import ClusterMappingArgs
    import pickle

    instance = ClusterMappingArgs(
        url="test",
        schema="test",
        db_nodes="test",
        cassandra_username="test",
        cassandra_password="test",
        ks_file="test",
        use_gs_lib_config_env="test",
        update="test",
    )

    pickled_instance = pickle.dumps(instance)

    assert isinstance(pickled_instance, bytes)
