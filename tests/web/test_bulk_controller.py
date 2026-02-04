# coding: utf-8

from tests.web import BaseTestCase
import graphsenselib.web.test.bulk_service as test_service


class TestBulkController(BaseTestCase):
    """BulkController integration test stubs"""

    async def test_bulk_csv(self):
        """Test case for bulk_csv

        Get data as CSV in bulk
        """
        await test_service.bulk_csv(self)

    async def test_bulk_json(self):
        """Test case for bulk_json

        Get data as JSON in bulk
        """
        await test_service.bulk_json(self)
