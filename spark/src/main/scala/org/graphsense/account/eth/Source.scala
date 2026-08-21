package org.graphsense.account.eth

import com.datastax.spark.connector.ColumnName
import org.apache.spark.sql.Dataset
import org.graphsense.account.{AccountSource, CassandraAccountSource, TokenSet}
import org.graphsense.account.eth.models.Trace
import org.graphsense.account.eth.tokens.EthTokenSet
import org.graphsense.storage.CassandraStorage

trait EthSource extends AccountSource {

  def traces(): Dataset[Trace]

}

class CassandraEthSource(store: CassandraStorage, keyspace: String)
    extends CassandraAccountSource(store, keyspace)
    with EthSource {

  override def getTokenSet(): TokenSet = {
    EthTokenSet
  }

  def traces(): Dataset[Trace] = {
    val spark = store.session()
    import spark.implicits._
    store.load[Trace](
      keyspace,
      "trace",
      Array(
        "block_id_group",
        "block_id",
        "trace_id",
        "trace_index",
        "from_address",
        "to_address",
        "value",
        "status",
        "call_type",
        "tx_hash"
      ).map(
        ColumnName(_)
      ): _*
    )
  }

}
