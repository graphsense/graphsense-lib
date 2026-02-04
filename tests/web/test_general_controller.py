# coding: utf-8

import pytest

from tests.web import BaseTestCase
import graphsenselib.web.test.general_service as test_service


class TestGeneralController(BaseTestCase):
    """GeneralController integration test stubs"""

    async def test_get_statistics(self):
        """Test case for get_statistics

        Get statistics of supported currencies
        """
        await test_service.get_statistics(self)

    async def test_search(self):
        """Test case for search

        Returns matching addresses, transactions and labels
        """
        await test_service.search(self)
