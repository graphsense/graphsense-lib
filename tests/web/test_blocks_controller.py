# coding: utf-8

from tests.web import BaseTestCase
import graphsenselib.web.test.blocks_service as test_service


class TestBlocksController(BaseTestCase):
    """BlocksController integration test stubs"""

    async def test_get_block(self):
        """Test case for get_block

        Get a block by its height
        """
        await test_service.get_block(self)

    async def test_list_block_txs(self):
        """Test case for list_block_txs

        Get block transactions
        """
        await test_service.list_block_txs(self)
