package org.graphsense.account.trx.models

case class Trace(
    // blockIdGroup: Int,
    blockId: Int,
    // internalIndex: Int,
    traceIndex: Int,
    callerAddress: Option[Array[Byte]],
    transfertoAddress: Option[Array[Byte]],
    // callInfoIndex: Int,
    callTokenId: Option[Int],
    callValue: BigInt,
    note: String,
    rejected: Boolean,
    txHash: Option[Array[Byte]]
)

case class Trc10(
    id: Int,
    ownerAddress: Array[Byte],
    name: String,
    abbr: String,
    description: String,
    totalSupply: BigInt,
    url: String,
    precision: Int
)

case class TxFee(
    txHashPrefix: String,
    txHash: Array[Byte],
    fee: Long,
    energyUsage: Long,
    energyFee: Long,
    originEnergyUsage: Long,
    energyUsageTotal: Long,
    netUsage: Long,
    netFee: Long,
    result: Int,
    energyPenaltyTotal: Long
)

// case class TrcFrozenSupply(
//     frozenAnmount: BigInt,
//     frozenDays:BigInt
// )
