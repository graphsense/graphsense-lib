# coding: utf-8

from tests.web import BaseTestCase
import graphsenselib.web.test.entities_service as test_service


class TestEntitiesController(BaseTestCase):
    """EntitiesController integration test stubs"""

    async def test_get_entity(self):
        """Test case for get_entity

        Get an entity
        """
        await test_service.get_entity(self)

    async def test_list_address_tags_by_entity(self):
        """Test case for list_address_tags_by_entity

        Get address tags for a given entity
        """
        await test_service.list_address_tags_by_entity(self)

    async def test_list_entity_addresses(self):
        """Test case for list_entity_addresses

        Get an entity's addresses
        """
        await test_service.list_entity_addresses(self)

    # async def test_list_entity_links(self):
    #    """Test case for list_entity_links
    #
    #    Get transactions between two entities
    #    """
    #    await test_service.list_entity_links(self)

    async def test_list_entity_neighbors(self):
        """Test case for list_entity_neighbors

        Get an entity's direct neighbors
        """
        await test_service.list_entity_neighbors(self)

    async def test_list_entity_txs(self):
        """Test case for list_entity_txs

        Get all transactions an entity has been involved in
        """
        await test_service.list_entity_txs(self)

    async def test_search_entity_neighbors(self):
        """Test case for search_entity_neighbors

        Search deeply for matching neighbors
        """
        await test_service.search_entity_neighbors(self)
