package org.graphsense.account.contract.tokens

import java.math.BigInteger
import org.graphsense.account.Implicits._
import org.graphsense.account.models.{Log, TokenTransfer}
import org.web3j.abi.{EventEncoder, FunctionReturnDecoder, TypeReference}
import org.web3j.abi.datatypes.Event
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object Erc20Decoder {

  val tokenTransferEvent: Event = new Event(
    "Transfer",
    List(
      TypeReference.makeTypeReference("address", true, false),
      TypeReference.makeTypeReference("address", true, false),
      TypeReference.makeTypeReference("uint256", false, false)
    ).asInstanceOf[List[TypeReference[_]]].asJava
  );

  val tokenTransferEventSelector = EventEncoder.encode(tokenTransferEvent)

  val transferTopicHash = hexStrToBytes(
    EventEncoder.encode(tokenTransferEvent)
  )

  def decodeTransfer(log: Log): Try[TokenTransfer] = {
    val topic0Str = bytesToHexStr(log.topic0)
    try {
      topic0Str match {
        case `tokenTransferEventSelector` => {
          val iparam = tokenTransferEvent.getIndexedParameters()
          val dparam = tokenTransferEvent.getNonIndexedParameters()
          val sender = FunctionReturnDecoder
            .decodeIndexedValue(
              bytesToHexStrCan(log.topics(1)),
              iparam.get(0)
            )
            .getValue()
            .asInstanceOf[String]
          val recipient = FunctionReturnDecoder
            .decodeIndexedValue(
              bytesToHexStrCan(log.topics(2)),
              iparam.get(1)
            )
            .getValue()
            .asInstanceOf[String]
          val value = BigInt(
            FunctionReturnDecoder
              .decode(bytesToHexStrCan(log.data), dparam)
              .get(0)
              .getValue()
              .asInstanceOf[BigInteger]
          )
          Success(
            TokenTransfer(
              log.blockId,
              log.transactionIndex,
              log.logIndex,
              log.txHash,
              log.address,
              sender,
              recipient,
              value
            )
          )
        }
        case _ => { Failure(new Exception("Wrong topic0, can't decode")) }
      }
    } catch {
      case e: Throwable => Failure(e)
    }
  }
}
