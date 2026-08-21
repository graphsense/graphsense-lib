package org.graphsense.account

import org.apache.spark.sql.Dataset
import org.graphsense.account.models.{
  Address,
  AddressIdByAddressPrefix,
  AddressIncomingRelationSecondaryIds,
  AddressOutgoingRelationSecondaryIds,
  AddressRelation,
  AddressTransaction,
  AddressTransactionSecondaryIds,
  Balance,
  BlockTransaction,
  Configuration,
  SummaryStatistics,
  TokenConfiguration,
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}
import org.graphsense.models.{ExchangeRates, TokenExchangeRates}
import org.graphsense.storage.CassandraStorage

trait AccountSink {
  def saveTokenConfiguration(tokenConf: Dataset[TokenConfiguration]): Unit
  def saveConfiguration(conf: Dataset[Configuration]): Unit
  def saveExchangeRates(rates: Dataset[ExchangeRates]): Unit
  def saveTokenExchangeRates(rates: Dataset[TokenExchangeRates]): Unit
  def saveTransactionIdsByGroup(
      ids: Dataset[TransactionIdByTransactionIdGroup]
  ): Unit
  def saveTransactionIdsByTxPrefix(
      ids: Dataset[TransactionIdByTransactionPrefix]
  ): Unit
  def saveAddressIdsByPrefix(ids: Dataset[AddressIdByAddressPrefix]): Unit
  def saveBalances(balances: Dataset[Balance]): Unit
  def saveBlockTransactions(
      blockTxs: Dataset[BlockTransaction]
  ): Unit
  def saveAddressTransactions(addressTxs: Dataset[AddressTransaction]): Unit
  def saveAddressTransactionBySecondaryId(
      ids: Dataset[AddressTransactionSecondaryIds]
  ): Unit
  def saveAddresses(addresses: Dataset[Address]): Unit
  def saveAddressIncomingRelations(relations: Dataset[AddressRelation]): Unit
  def saveAddressOutgoingRelations(relations: Dataset[AddressRelation]): Unit
  def saveAddressOutgoingRelationsBySecondaryId(
      ids: Dataset[AddressOutgoingRelationSecondaryIds]
  ): Unit
  def saveAddressIncomingRelationsBySecondaryId(
      ids: Dataset[AddressIncomingRelationSecondaryIds]
  ): Unit
  def saveSummaryStatistics(statistic: Dataset[SummaryStatistics]): Unit

  def areTransactionIdsGroupEmpty(): Boolean
  def areTransactionIdsPrefixEmpty(): Boolean
  def areAddressIdsEmpty(): Boolean
  def areBalancesEmpty(): Boolean
  def areBlockTransactionsEmpty(): Boolean
  def areAddressTransactionsEmtpy(): Boolean
  def areAddressTransactionsSecondaryGroupEmtpy(): Boolean
  def areAddressEmpty(): Boolean
  def areAddressIncomingRelationsEmpty(): Boolean
  def areAddressOutgoingRelationsEmpty(): Boolean
  def areAddressIncomingRelationsSecondaryIdsEmpty(): Boolean
  def areAddressOutgoingRelationsSecondaryIdsEmpty(): Boolean
}

class CassandraAccountSink(store: CassandraStorage, keyspace: String)
    extends AccountSink {

  def saveConfiguration(conf: Dataset[Configuration]): Unit = {
    store.store(
      keyspace,
      "configuration",
      conf
    )
  }

  def saveTokenConfiguration(tokenConf: Dataset[TokenConfiguration]): Unit = {
    store.store(
      keyspace,
      "token_configuration",
      tokenConf
    )
  }

  def saveExchangeRates(rates: Dataset[ExchangeRates]): Unit = {
    store.store(keyspace, "exchange_rates", rates)
  }

  def saveTokenExchangeRates(rates: Dataset[TokenExchangeRates]): Unit = {
    // The table is added by a graphsense-lib schema migration; tolerate
    // transformed keyspaces that have not been migrated yet.
    if (store.tableExists(keyspace, "token_exchange_rates")) {
      store.store(keyspace, "token_exchange_rates", rates)
    } else {
      println(
        s"WARNING: table ${keyspace}.token_exchange_rates does not exist, " +
          "skipping token exchange rates"
      )
    }
  }

  override def areTransactionIdsGroupEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "transaction_ids_by_transaction_id_group")
  }

  override def saveTransactionIdsByGroup(
      ids: Dataset[TransactionIdByTransactionIdGroup]
  ): Unit = {
    store.store(
      keyspace,
      "transaction_ids_by_transaction_id_group",
      ids
    )
  }

  def areTransactionIdsPrefixEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "transaction_ids_by_transaction_prefix")
  }

  override def saveTransactionIdsByTxPrefix(
      ids: Dataset[TransactionIdByTransactionPrefix]
  ): Unit = {
    store.store(
      keyspace,
      "transaction_ids_by_transaction_prefix",
      ids
    )
  }

  def areAddressIdsEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "address_ids_by_address_prefix")
  }

  override def saveAddressIdsByPrefix(
      ids: Dataset[AddressIdByAddressPrefix]
  ): Unit = {
    store.store(
      keyspace,
      "address_ids_by_address_prefix",
      ids
    )
  }

  def areBalancesEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "balance")
  }

  override def saveBalances(balances: Dataset[Balance]): Unit = {
    store.store(keyspace, "balance", balances)
  }

  override def areBlockTransactionsEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "block_transactions")
  }

  def saveBlockTransactions(
      blockTxs: Dataset[BlockTransaction]
  ): Unit = {
    store.store(
      keyspace,
      "block_transactions",
      blockTxs
    )
  }

  override def areAddressTransactionsEmtpy(): Boolean = {
    store.isTableEmpty(keyspace, "address_transactions")
  }

  override def saveAddressTransactions(
      addressTxs: Dataset[AddressTransaction]
  ): Unit = {
    store.store(
      keyspace,
      "address_transactions",
      addressTxs
    )
  }

  override def areAddressTransactionsSecondaryGroupEmtpy(): Boolean = {
    store.isTableEmpty(keyspace, "address_transactions_secondary_ids")
  }

  override def saveAddressTransactionBySecondaryId(
      ids: Dataset[AddressTransactionSecondaryIds]
  ): Unit = {
    store.store(
      keyspace,
      "address_transactions_secondary_ids",
      ids
    )
  }

  override def areAddressEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "address")
  }

  override def saveAddresses(addresses: Dataset[Address]): Unit = {
    store.store(keyspace, "address", addresses)
  }

  override def areAddressIncomingRelationsEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "address_incoming_relations")
  }

  override def saveAddressIncomingRelations(
      relations: Dataset[AddressRelation]
  ): Unit = {
    store.store(keyspace, "address_incoming_relations", relations)
  }

  override def areAddressOutgoingRelationsEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "address_outgoing_relations")
  }
  override def saveAddressOutgoingRelations(
      relations: Dataset[AddressRelation]
  ): Unit = {
    store.store(keyspace, "address_outgoing_relations", relations)
  }

  override def areAddressOutgoingRelationsSecondaryIdsEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "address_outgoing_relations_secondary_ids")
  }
  override def saveAddressOutgoingRelationsBySecondaryId(
      ids: Dataset[AddressOutgoingRelationSecondaryIds]
  ): Unit = {
    store.store(
      keyspace,
      "address_outgoing_relations_secondary_ids",
      ids
    )
  }

  override def areAddressIncomingRelationsSecondaryIdsEmpty(): Boolean = {
    store.isTableEmpty(keyspace, "address_incoming_relations_secondary_ids")
  }
  override def saveAddressIncomingRelationsBySecondaryId(
      ids: Dataset[AddressIncomingRelationSecondaryIds]
  ): Unit = {
    store.store(
      keyspace,
      "address_incoming_relations_secondary_ids",
      ids
    )
  }

  override def saveSummaryStatistics(
      statistic: Dataset[SummaryStatistics]
  ): Unit = {
    store.store(
      keyspace,
      "summary_statistics",
      statistic
    )
  }

}
