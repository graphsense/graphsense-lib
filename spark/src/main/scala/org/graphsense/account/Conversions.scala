package org.graphsense.account
import scala.language.implicitConversions

object Implicits {
  implicit def hexStrToBytes(s: String): Array[Byte] = {
    val hex = canonicalHex(s)
    assert(hex.length % 2 == 0, "Hex string has wrong length")
    hex.sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toArray
  }

  def canonicalHex(s: String): String = {
    s.stripPrefix("0x").toLowerCase()
  }

  implicit def hexstrToBigInt(s: String): BigInt = {
    BigInt(canonicalHex(s), 16)
  }

  implicit def bytesToHexStr(bytes: Array[Byte]): String = {
    val sb = new StringBuilder
    for (b <- bytes) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    "0x" + sb.toString
  }

  def bytesToHexStrCan(bytes: Array[Byte]): String = {
    canonicalHex(bytesToHexStr(bytes))
  }

}
