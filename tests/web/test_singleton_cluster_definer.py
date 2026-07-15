"""Closes #62: an address tag on a size-1 cluster is auto-promoted to a
cluster tag, even when ``is_cluster_definer`` is unset.

This is handled purely at read time by the ``best_cluster_tag`` materialized
view (``tagstore/db/init.sql``): its second UNION branch promotes address tags
whose cluster has ``gs_cluster_no_addr = 1`` regardless of the
``is_cluster_definer`` flag. No stored flag mutation is needed.

The web suite's seeded fixtures (``tests/web/tagstore/data/data.sql`` +
``tagpack_public.yaml``, loaded and ``refresh-views``'d by ``gs_rest_db_setup``)
already encode the scenario:

* cluster 19 (``tag_addressH``) - ``gs_cluster_no_addr = 1``, single address tag
  with ``is_cluster_definer`` unset  -> must be promoted.
* cluster 20 (``tag_addressI``) - ``gs_cluster_no_addr = 1``, three address tags,
  none a definer                     -> must be promoted.
* cluster 12 (``tag_addressA``/``B``) - ``gs_cluster_no_addr = 2``, tags with
  ``is_cluster_definer`` unset        -> must NOT be promoted (negative control
  that distinguishes the size-1 rule from blanket promotion).
"""

import pytest
import pytest_asyncio

from graphsenselib.tagstore.db.queries import TagstoreDbAsync


@pytest_asyncio.fixture
async def seeded_tagstore(gs_rest_db_setup):
    """Async tagstore bound to the web suite's seeded Postgres.

    ``gs_rest_db_setup`` is a session-scoped shared config dict; the ``client``
    fixture parses its ``gs-tagstore`` entry into a ``TagStoreReaderConfig`` in
    place, so depending on fixture ordering this entry is either a raw
    ``{"url": ...}`` dict or a parsed config object. Handle both.
    """
    tagstore_cfg = gs_rest_db_setup["gs-tagstore"]
    url = tagstore_cfg.url if hasattr(tagstore_cfg, "url") else tagstore_cfg["url"]
    db = TagstoreDbAsync.from_url(url)
    try:
        yield db
    finally:
        await db.engine.dispose()


@pytest.mark.asyncio
async def test_singleton_cluster_promotes_non_definer_address_tag(seeded_tagstore):
    db = seeded_tagstore

    # cluster 19: singleton, non-definer address tag -> promoted to cluster tag.
    tag_h = await db.get_best_cluster_tag(19, "BTC", ["public"])
    assert tag_h is not None
    assert tag_h.label == "x"

    # cluster 20: singleton with several non-definer tags -> still promoted.
    tag_i = await db.get_best_cluster_tag(20, "BTC", ["public"])
    assert tag_i is not None


@pytest.mark.asyncio
async def test_multi_address_cluster_without_definer_is_not_promoted(seeded_tagstore):
    db = seeded_tagstore

    # cluster 12 has 2 addresses and no cluster-definer tag: the size-1 rule must
    # not fire, so there is no best cluster tag.
    assert await db.get_best_cluster_tag(12, "BTC", ["public"]) is None
