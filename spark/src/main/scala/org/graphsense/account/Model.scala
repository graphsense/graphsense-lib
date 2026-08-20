package org.graphsense.account.models

import org.graphsense.account.Implicits._

// lookup tables

package object types {
  type TransactionIdType = Long
  type AddressIdType = Int
}

case class TransactionId(
    transaction: Array[Byte],
    transactionId: types.TransactionIdType
)

case class TransactionIdByTransactionPrefix(
    transactionPrefix: String,
    transaction: Array[Byte],
    transactionId: types.TransactionIdType
)

case class TransactionIdByTransactionIdGroup(
    transactionIdGroup: Int,
    transactionId: types.TransactionIdType,
    transaction: Array[Byte]
)

case class AddressId(
    address: Array[Byte],
    addressId: types.AddressIdType
)

case class AddressIdLong(
    address: Array[Byte],
    addressId: Long
)

case class AddressIdByAddressPrefix(
    addressPrefix: String,
    address: Array[Byte],
    addressId: types.AddressIdType
)

// Helper types

case class TokenConfiguration(
    currencyTicker: String,
    tokenAddress: Array[Byte],
    standard: String,
    decimals: Int,
    decimalDivisor: Long,
    pegCurrency: Option[String]
)

case class TokenTransfer(
    blockId: Int,
    transactionIndex: Short,
    logIndex: Int,
    txHash: Array[Byte],
    tokenAddress: Array[Byte],
    from: Array[Byte],
    to: Array[Byte],
    value: BigInt
) {
  override def equals(thatGeneric: scala.Any): Boolean = {
    if (!thatGeneric.isInstanceOf[TokenTransfer])
      false
    else {
      val that = thatGeneric.asInstanceOf[TokenTransfer]
      bytesToHexStr(that.txHash) == bytesToHexStr(
        this.txHash
      ) && bytesToHexStr(that.tokenAddress) == bytesToHexStr(
        this.tokenAddress
      ) && bytesToHexStr(that.from) == bytesToHexStr(
        this.from
      ) && bytesToHexStr(that.to) == bytesToHexStr(
        this.to
      ) && (that.value == this.value
        && that.blockId == this.blockId
        && that.logIndex == this.logIndex
        && that.transactionIndex == this.transactionIndex)

    }

  }

  /** TODO fix hashcode in case I want to use this in collection
    * @return
    */
  override def hashCode(): Int = super.hashCode()

  def isDefault(): Boolean = {
    this == TokenTransfer.default()
  }
}

object TokenTransfer {
  def default() = new TokenTransfer(
    0,
    0,
    0,
    hexStrToBytes(
      "0x0000000000000000000000000000000000000000000000000000000000000000"
    ),
    hexStrToBytes("0x0000000000000000000000000000000000000000"),
    hexStrToBytes("0x0000000000000000000000000000000000000000"),
    hexStrToBytes("0x0000000000000000000000000000000000000000"),
    BigInt.int2bigInt(0)
  )
}

case class Contract(
    addressId: Int
)

// transformed schema data types

case class TxReference(traceIndex: Option[Int], logIndex: Option[Int])

// raw schema tables

case class Currency(value: BigInt, fiatValues: Seq[Float])

case class Block(
    blockIdGroup: Int,
    blockId: Int,
    blockHash: Array[Byte],
    parentHash: Array[Byte],
    nonce: Array[Byte],
    sha3_uncles: Array[Byte],
    logsBloom: Array[Byte],
    transactionsRoot: Array[Byte],
    stateRoot: Array[Byte],
    receiptsRoot: Array[Byte],
    miner: Array[Byte],
    difficulty: Option[BigInt],
    totalDifficulty: Option[BigInt],
    size: Int,
    extraData: Array[Byte],
    gasLimit: Int,
    gasUsed: Int,
    baseFeePerGas: Option[Long],
    timestamp: Int,
    transactionCount: Short
)

case class Transaction(
    txHashPrefix: String,
    txHash: Array[Byte],
    nonce: Int,
    blockHash: Array[Byte],
    blockId: Int,
    transactionIndex: Short,
    fromAddress: Option[Array[Byte]],
    toAddress: Option[Array[Byte]],
    value: BigInt, // in wei, 1e18 wei = 1 ether
    gas: Int,
    gasPrice: BigInt,
    input: Array[Byte],
    blockTimestamp: Int,
    receiptGasUsed: BigInt,
    receiptStatus: Option[BigInt],
    receiptContractAddress: Option[Array[Byte]]
)

case class Log(
    blockIdGroup: Int,
    blockId: Int,
    blockHash: Array[Byte],
    address: Array[Byte],
    data: Array[Byte],
    topics: Seq[Array[Byte]],
    topic0: Array[Byte],
    txHash: Array[Byte],
    logIndex: Int,
    transactionIndex: Short
)

// transformed schema tables

case class Balance(
    addressIdGroup: Int,
    addressId: types.AddressIdType,
    balance: BigInt,
    currency: String
)

// case class BlockTransaction(
//     blockIdGroup: Int,
//     blockId: Int,
//     txs: Seq[types.TransactionIdType]
// )
case class BlockTransaction(
    blockIdGroup: Int,
    blockId: Int,
    txId: types.TransactionIdType
)

case class EncodedTokenTransfer(
    transactionId: types.TransactionIdType,
    logIndex: Int,
    blockId: Int,
    currency: String,
    srcAddressId: types.AddressIdType,
    dstAddressId: Option[types.AddressIdType],
    value: BigInt,
    fiatValues: Seq[Float]
)

case class EncodedTransaction(
    transactionId: types.TransactionIdType,
    // nonce: Int,
    blockId: Int,
    traceIndex: Option[Int],
    // transactionIndex: Short,
    srcAddressId: types.AddressIdType,
    dstAddressId: Option[types.AddressIdType],
    value: BigInt,
    fiatValues: Seq[Float]
    // gas: Int,
    // gasPrice: BigInt,
    // input: Array[Byte],
    // blockTimestamp: Int
)

case class AddressTransaction(
    addressIdGroup: Int,
    addressIdSecondaryGroup: Int,
    addressId: types.AddressIdType,
    transactionId: types.TransactionIdType,
    txReference: TxReference,
    currency: String,
    isOutgoing: Boolean
)

case class AddressTransactionSecondaryIds(
    addressIdGroup: Int,
    maxSecondaryId: Int
)

case class Address(
    addressIdGroup: Int,
    addressId: types.AddressIdType,
    address: Array[Byte],
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    noIncomingTxsZeroValue: Int,
    noOutgoingTxsZeroValue: Int,
    firstTxId: types.TransactionIdType,
    lastTxId: types.TransactionIdType,
    totalReceived: Currency,
    totalSpent: Currency,
    totalTokensReceived: Map[String, Currency],
    totalTokensSpent: Map[String, Currency],
    inDegree: Int,
    outDegree: Int,
    inDegreeZeroValue: Int,
    outDegreeZeroValue: Int,
    isContract: Boolean
)

case class AddressRelation(
    srcAddressIdGroup: Int,
    srcAddressIdSecondaryGroup: Int,
    srcAddressId: types.AddressIdType,
    dstAddressIdGroup: Int,
    dstAddressIdSecondaryGroup: Int,
    dstAddressId: types.AddressIdType,
    noTransactions: Int,
    value: Currency,
    tokenValues: Map[String, Currency]
)

case class AddressOutgoingRelationSecondaryIds(
    srcAddressIdGroup: Int,
    maxSecondaryId: Int
)

case class AddressIncomingRelationSecondaryIds(
    dstAddressIdGroup: Int,
    maxSecondaryId: Int
)

case class SummaryStatistics(
    id: Int,
    timestamp: Int,
    timestampTransform: Int,
    noBlocks: Long,
    noBlocksTransform: Long,
    noTransactions: Long,
    noAddresses: Long,
    noAddressRelations: Long
)

case class Configuration(
    keyspaceName: String,
    bucketSize: Int,
    blockBucketSizeAddressTxs: Int,
    addressrelationsIdsNbuckets: Int,
    txPrefixLength: Int,
    addressPrefixLength: Int,
    fiatCurrencies: Seq[String]
)
