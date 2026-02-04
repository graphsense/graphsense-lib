# coding: utf-8

from tests.web import BaseTestCase
import graphsenselib.web.test.tokens_service as test_service


class TestTokensController(BaseTestCase):
    """TokensController integration test stubs"""

    async def test_list_supported_tokens(self):
        """Test case for list_supported_tokens

        Returns a list of supported token (sub)currencies.
        """
        await test_service.list_supported_tokens(self)
