# coding: utf-8

from tests.web import BaseTestCase
import graphsenselib.web.test.tags_service as test_service


class TestTagsController(BaseTestCase):
    """TagsController integration test stubs"""

    async def test_get_actor(self):
        """Test case for get_actor

        Returns an actor given its unique id or (unique) label
        """
        await test_service.get_actor(self)

    async def test_get_actor_tags(self):
        """Test case for get_actor_tags

        Returns the address tags for a given actor
        """
        await test_service.get_actor_tags(self)

    async def test_list_address_tags(self):
        """Test case for list_address_tags

        Returns address tags associated with a given label
        """
        await test_service.list_address_tags(self)

    async def test_list_concepts(self):
        """Test case for list_concepts

        Returns the supported concepts of a taxonomy
        """
        await test_service.list_concepts(self)

    async def test_list_taxonomies(self):
        """Test case for list_taxonomies

        Returns the supported taxonomies
        """
        await test_service.list_taxonomies(self)
