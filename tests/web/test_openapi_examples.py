"""Guard the hand-maintained OpenAPI examples on the web models.

Every `api_model_config(EXAMPLE)` example is published verbatim in
`openapi.json` right next to the schema it is supposed to illustrate, but
nothing forced the two to agree. A field rename on the model (e.g.
`schema_type` -> `network_type` on `CurrencyStats`) silently left the example
behind, so the spec shipped an example that violated its own schema.

Two gates:

1. Every example must validate against its own model. Hard failure, no
   exceptions.
2. Every example should exercise every declared field. Because a handful of
   examples are deliberately partial today, the *missing* half of that check
   runs against `KNOWN_GAPS`, which is compared for exact equality: adding a
   field without extending the example fails, and covering an allowlisted
   field also fails until it is removed from the list. The baseline can only
   shrink.
"""

import importlib
import pkgutil

import pytest

import graphsenselib.web.models as models_pkg
from graphsenselib.web.models.base import APIModel

# Fields declared on the model but absent from its example, as of the
# 2026-07-28 audit. Shrink-only: never add an entry to silence a new model,
# extend that model's example instead. Exception: external-backend contract
# extensions (capabilities, truncation qualifiers) whose ABSENCE is the
# semantic default — baseline responses never carry them, so the canonical
# examples must not show them either.
KNOWN_GAPS: dict[str, set[str]] = {
    "Address": {
        "actors",
        "aggregates_truncated",
        "cutoff",
        "is_contract",
        "tags",
        "token_balances",
        "total_tokens_received",
        "total_tokens_spent",
    },
    "AddressTag": {"abuse", "actor", "concepts", "inherited_from"},
    "Cluster": {
        "actors",
        "best_address_tag",
        "token_balances",
        "total_tokens_received",
        "total_tokens_spent",
    },
    "CurrencyStats": {
        "coin_decimals",
        "coin_ticker",
        "network_name",
    },
    "Entity": {
        "actors",
        "best_address_tag",
        "token_balances",
        "total_tokens_received",
        "total_tokens_spent",
    },
    "Links": {"next_page"},
    "NeighborAddress": {"token_values"},
    "NeighborCluster": {"token_values"},
    "NeighborEntity": {"token_values"},
    "SearchResult": {"actors"},
    "TagSummary": {"best_actor", "best_label", "tag_count_indirect"},
    "TokenConfig": {"contract_address"},
    "TxAccount": {"contract_creation", "fee", "is_external", "token_tx_id"},
    "TxUtxo": {"heuristics"},
}

# Discovery guard: if a refactor moves examples somewhere `_models_with_examples`
# cannot see, the parametrised tests would pass vacuously. 29 models carry an
# example today.
MIN_EXPECTED_MODELS = 20


def _models_with_examples() -> list[type[APIModel]]:
    for module in pkgutil.iter_modules(models_pkg.__path__):
        importlib.import_module(f"{models_pkg.__name__}.{module.name}")

    def subclasses(cls):
        for sub in cls.__subclasses__():
            yield sub
            yield from subclasses(sub)

    models = []
    for model in sorted(set(subclasses(APIModel)), key=lambda c: c.__name__):
        extra = model.model_config.get("json_schema_extra") or {}
        if isinstance(extra, dict) and extra.get("example") is not None:
            models.append(model)
    return models


MODELS = _models_with_examples()


def _declared_keys(model: type[APIModel]) -> set[str]:
    keys = set(model.model_fields) | set(model.model_computed_fields)
    keys |= {f.alias for f in model.model_fields.values() if f.alias}
    return keys


def _example(model: type[APIModel]) -> dict:
    return model.model_config["json_schema_extra"]["example"]


def test_discovery_found_the_examples():
    assert len(MODELS) >= MIN_EXPECTED_MODELS, (
        f"only {len(MODELS)} models with examples discovered; examples were "
        "probably moved out of model_config and these tests are now vacuous"
    )


@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__name__)
def test_example_is_valid_for_its_model(model):
    # Raises ValidationError with the offending field if the example drifted.
    model.model_validate(_example(model))


@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__name__)
def test_example_declares_no_unknown_fields(model):
    unknown = set(_example(model)) - _declared_keys(model)
    assert not unknown, (
        f"{model.__name__} example has keys the model does not declare: "
        f"{sorted(unknown)}"
    )


@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__name__)
def test_example_field_coverage_matches_baseline(model):
    missing = _declared_keys(model) - set(_example(model))
    expected = KNOWN_GAPS.get(model.__name__, set())
    assert missing == expected, (
        f"{model.__name__} example coverage changed: "
        f"newly uncovered={sorted(missing - expected)}, "
        f"now covered (drop from KNOWN_GAPS)={sorted(expected - missing)}"
    )


def test_known_gaps_has_no_stale_entries():
    stale = set(KNOWN_GAPS) - {m.__name__ for m in MODELS}
    assert not stale, (
        f"KNOWN_GAPS names models that no longer carry an example: {sorted(stale)}"
    )
