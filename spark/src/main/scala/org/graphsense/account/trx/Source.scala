package org.graphsense.account.trx

import com.datastax.spark.connector.ColumnName
import org.apache.spark.sql.Dataset
import org.graphsense.account.{AccountSource, CassandraAccountSource, TokenSet}
import org.apache.spark.sql.functions.{col, length, when}
import org.graphsense.account.trx.models.{Trace, Trc10, TxFee}
import org.graphsense.account.trx.tokens.TrxTokenSet
import org.graphsense.storage.CassandraStorage

trait TrxSource extends AccountSource {

  def traces(): Dataset[Trace]

  def trc10Tokens(): Dataset[Trc10]

  def txFee(): Dataset[TxFee]

}

class CassandraTrxSource(store: CassandraStorage, keyspace: String)
    extends CassandraAccountSource(store, keyspace)
    with TrxSource {

  override def getTokenSet(): TokenSet = {
    TrxTokenSet
  }

  def txFee(): Dataset[TxFee] = {
    val spark = store.session()
    import spark.implicits._
    store.load[TxFee](
      keyspace,
      "fee"
    )
  }

  def trc10Tokens(): Dataset[Trc10] = {
    val spark = store.session()
    import spark.implicits._
    store.load[Trc10](
      keyspace,
      "trc10",
      Array(
        "id",
        "ownerAddress",
        "name",
        "abbr",
        "description",
        "totalSupply",
        "url",
        "precision"
      ).map(
        ColumnName(_)
      ): _*
    )
  }

  def traces(): Dataset[Trace] = {
    val spark = store.session()
    import spark.implicits._
    /*
      transferto_address, can hold 0x instead of null values thus we need to transform to keep the logic
       select * from trace where block_id = 50718672 and block_id_group = 50718;
     */
    store
      .load[Trace](
        keyspace,
        "trace",
        Array(
          "block_id",
          "trace_index",
          "caller_address",
          "transferto_address",
          "call_token_id",
          "call_value",
          "note",
          "rejected",
          "tx_hash"
        ).map(
          ColumnName(_)
        ): _*
      )
      .withColumn(
        "transfertoAddress",
        when(length(col("transfertoAddress")) === 0, null)
          .otherwise(col("transfertoAddress"))
      )
      .as[Trace]
  }
}
