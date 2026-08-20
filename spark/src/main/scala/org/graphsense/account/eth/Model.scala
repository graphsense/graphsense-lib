package org.graphsense.account.eth.models

case class Trace(
    blockIdGroup: Int,
    blockId: Int,
    traceId: String,
    traceIndex: Int,
    fromAddress: Option[Array[Byte]],
    toAddress: Option[Array[Byte]],
    value: BigInt,
    status: Int,
    callType: Option[String],
    txHash: Option[Array[Byte]]
)
