# coding: utf-8

from tests.web import BaseTestCase
import graphsenselib.web.test.txs_service as test_service


class TestTxsController(BaseTestCase):
    """TxsController integration test stubs"""

    async def test_get_spending_txs(self):
        """Test case for get_spending_txs

        Returns in which other transaction's outputs the asked transaction spent. Think backwards references is the transaction graph. This endpoint is only available for utxo like currencies.
        """
        await test_service.get_spending_txs(self)

    async def test_get_spent_in_txs(self):
        """Test case for get_spent_in_txs

        Returns in which other transactions, outputs from the asked transaction are spent. Think forward references in the transaction graph. This endpoint is only available for utxo like currencies.
        """
        await test_service.get_spent_in_txs(self)

    async def test_get_tx(self):
        """Test case for get_tx

        Returns details of a specific transaction identified by its hash.
        """
        await test_service.get_tx(self)

    async def test_get_tx_io(self):
        """Test case for get_tx_io

        Returns input/output values of a specific transaction identified by its hash.
        """
        await test_service.get_tx_io(self)

    async def test_list_token_txs(self):
        """Test case for list_token_txs

        Returns all token transactions in a given transaction
        """
        await test_service.list_token_txs(self)
