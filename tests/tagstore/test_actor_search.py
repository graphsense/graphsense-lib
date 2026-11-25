"""Actor search ranks exact, prefix and word matches above look-alikes.

Plain trigram similarity on the label returned "BTFinance" and friends for
"binance" and missed "Binance Multi Chain Capital"; aliases and the query's
word boundaries were ignored. See _ACTOR_SEARCH_SQL in tagstore/db/queries.py.
"""

import pytest
from sqlalchemy import text
from sqlmodel.ext.asyncio.session import AsyncSession

from graphsenselib.tagstore.db import queries as q


def test_params_normalize_like_the_stored_terms():
    p = q._actor_search_params("  Binance.US ")
    assert p["q"] == "binanceus"
    assert p["q_words"] == "binance us"


def test_params_keep_like_wildcards_out_of_the_word_pattern():
    p = q._actor_search_params("100%_x")
    assert p["q"] == "100x"
    assert p["q_words"] == "100 x"


def test_params_substring_tier_only_for_longer_queries():
    assert q._actor_search_params("okx")["allow_substring"] is False
    assert q._actor_search_params("swap")["allow_substring"] is True


@pytest.mark.parametrize("query", ["", "   ", "---", "_"])
def test_nothing_searchable_means_no_statement(query):
    assert q._get_similar_actors_stmt(query, 10) is None


ACTORS = [
    # id, label, context
    ("binance_t", "Binance T", '{"aliases": ["binanceus", "binance_us"]}'),
    ("binancemcc_t", "Binance Multi Chain Capital T", None),
    ("btfinance_t", "BTFinance T", None),
    ("boofinance_t", "BooFinance T", None),
    ("lazarus_t", "Lazarus Group T", None),
    ("uniswap_t", "Uniswap T", None),
    ("tornado_t", "Tornado Cash T", '{"aliases": ["tornadocash"]}'),
    ("hfione_t", "Hfione", None),
    ("broken_ctx_t", "Broken Context T", "not json {"),
    ("alias_not_list_t", "Alias Not List T", '{"aliases": "x"}'),
]


@pytest.fixture
async def actor_search(async_tagstore_db):
    """Search over ACTORS on top of the shared test data, rolled back after."""
    async with async_tagstore_db.engine.connect() as conn:
        trans = await conn.begin()
        try:
            pack = (await conn.execute(text("SELECT id FROM actorpack LIMIT 1"))).one()[
                0
            ]
            await conn.execute(
                text(
                    "INSERT INTO actor (id, label, context, actorpack) "
                    "VALUES (:id, :label, :context, :pack)"
                ),
                [
                    {"id": i, "label": lbl, "context": ctx, "pack": pack}
                    for i, lbl, ctx in ACTORS
                ],
            )
            session = AsyncSession(bind=conn)

            async def search(query, limit=10, only_ours=True):
                res = await async_tagstore_db.search_actor_labels(
                    query, limit, session=session
                )
                return [r.id for r in res if r.id.endswith("_t") or not only_ours]

            yield search
        finally:
            await trans.rollback()


@pytest.mark.asyncio
async def test_exact_and_prefix_beat_look_alikes(actor_search):
    assert await actor_search("binance t") == ["binance_t"]
    res = await actor_search("binance")
    assert res[:2] == ["binance_t", "binancemcc_t"]
    assert "btfinance_t" not in res and "boofinance_t" not in res


@pytest.mark.asyncio
async def test_aliases_match(actor_search):
    assert (await actor_search("Binance.US"))[0] == "binance_t"
    assert (await actor_search("binance_us"))[0] == "binance_t"
    assert (await actor_search("tornadocash"))[0] == "tornado_t"


@pytest.mark.asyncio
async def test_word_and_substring_matches(actor_search):
    assert (await actor_search("group"))[0] == "lazarus_t"
    assert "uniswap_t" in await actor_search("swap")
    # substring only from 4 characters on
    assert "uniswap_t" not in await actor_search("wap")


@pytest.mark.asyncio
async def test_fuzzy_only_as_a_fallback(actor_search):
    # nothing matches directly: a typo still finds the actor
    # ("binance" from the shared test actorpack) ...
    assert "binance" in await actor_search("binnace", only_ours=False)
    assert "hfione_t" in await actor_search("huione")


@pytest.mark.asyncio
async def test_bad_context_does_not_break_the_search(actor_search):
    assert (await actor_search("broken context t"))[0] == "broken_ctx_t"
    assert (await actor_search("alias not list t"))[0] == "alias_not_list_t"
