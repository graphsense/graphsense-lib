package org.graphsense

import scala.collection.Map
import math.{ceil, log10}

/*Allows The decoding of addresses encoded in binary form in the delta lake tables written by gslib*/

object AddressDecoder {

  val b58pp = toAlphabet(
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
  )
  val bech32 = toAlphabet("qpzry9x8gf2tvdw0s3jn54khce6mua7lb1")
  val base62 = toAlphabet(
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  )

  val bech32Prefix = Map(
    ("btc", bytesFromHex("859880")),
    ("ltc", bytesFromHex("80c662"))
  )

  val nonstandardPrefix = bytesFromHex("cb3cb7e25ca8976a00")

  val log2 = (x: Int) => log10(x.toDouble) / log10(2.0)

  def bytesFromHex(hex: String): Array[Byte] = {
    assert(hex.length % 2 == 0) // only manage canonical case
    hex.sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toArray
  }

  def toAlphabet(string: String): Map[Byte, Char] = string.zipWithIndex.map {
    case (c, i) => ((i + 1).toByte, c)
  }.toMap

  def isBitPrefix(data: Array[Byte], prefix: Array[Byte]): Boolean = {
    !prefix.zipWithIndex
      .map { case (b, i) => (b & data(i)).toByte == b }
      .contains(false)

  }

  def selectNBits(b: Byte, s: Int, n: Int): Byte = {
    assert(s + n <= 8)
    val mask = (((1 << n) - 1) << (8 - s - n))

    return ((b & mask) >> (8 - s - n)).toByte
  }

  def getNBitsFromByteArray(data: Array[Byte], n: Int, bitwidth: Int): Byte = {
    val bitpos = n * bitwidth;
    val bytepos = bitpos / 8
    val b1 = data(bytepos)
    val b2: Byte = data.lift(bytepos + 1).getOrElse(0.toByte)

    val fbo = bitpos % 8
    val fbr = Math.min(8 - fbo, bitwidth)
    val sbr = Math.max(bitwidth - fbr, 0)

    val res1 = selectNBits(b1, fbo, fbr) << sbr
    val res2 = selectNBits(b2, 0, sbr)

    return (res1.toByte + res2).toByte
  }

  def decodeWithAlphabet(
      b: Array[Byte],
      alphabet_lookup: Map[Byte, Char]
  ): String = {
    val bitWidth = ceil(log2(alphabet_lookup.size)).toInt
    Range(0, b.length * 8 / bitWidth)
      .map {
        case x => {
          val a = getNBitsFromByteArray(b, x, bitWidth)
          if (a == 0) "" else alphabet_lookup(a)
        }
      }
      .mkString("")
  }

  def decodeBase58(b: Array[Byte]): String = {
    decodeWithAlphabet(b, b58pp)
  }

  def decodeBech32(b: Array[Byte]): String = {
    decodeWithAlphabet(b, bech32)
  }

  def decodeBase62(b: Array[Byte]): String = {
    // this is only needed for nonstandard addresses... (with prefix nonstandard)
    decodeWithAlphabet(b, base62)
  }

  def decodeHex(b: Array[Byte]): String = {
    b.map("%02X" format _).mkString.toLowerCase()
  }

  def decodeTron(b: Array[Byte]): String = {
    val tronWithPrefix = 0x41.toByte +: b
    val checksum = Util.sha256(Util.sha256(tronWithPrefix)).slice(0, 4)
    return Util.encodeBase58(tronWithPrefix ++ checksum)
  }

  def decodeUtxo(
      address: Array[Byte],
      bech32_prefix: Option[Array[Byte]]
  ): String = {
    if (isBitPrefix(address, nonstandardPrefix)) {
      decodeBase62(address)
    } else {
      bech32_prefix match {
        case Some(prefix) if isBitPrefix(address, prefix) =>
          decodeBech32(address)
        case _ => decodeBase58(address)
      }

    }

  }

  def address_to_str(network: String, address: Array[Byte]): String = {
    network.toLowerCase match {
      case "eth" => decodeHex(address)
      case "trx" => decodeTron(address)
      case "btc" | "zec" | "bch" | "zec" | "ltc" =>
        decodeUtxo(address, bech32Prefix.get(network.toLowerCase))
      case _ => throw new Exception("Unknown network: " + network)
    }

  }

}
