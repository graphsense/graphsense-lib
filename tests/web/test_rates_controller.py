# coding: utf-8

from tests.web import BaseTestCase
import graphsenselib.web.test.rates_service as test_service


class TestRatesController(BaseTestCase):
    """RatesController integration test stubs"""

    async def test_get_exchange_rates(self):
        """Test case for get_exchange_rates

        Returns exchange rate for a given height
        """
        await test_service.get_exchange_rates(self)
