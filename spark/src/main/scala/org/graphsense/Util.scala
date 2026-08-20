package org.graphsense

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.security.MessageDigest
import org.apache.spark.sql.Dataset

/*import org.apache.spark.sql.functions.{col, when}*/

object Util {

  import java.util.concurrent.TimeUnit

  def printDatasetStats[T](df: Dataset[T], name: String) = {
    println(f"Query Plan -- ${name}")
    df.explain()
    println(f"Schema  -- ${name}")
    df.printSchema()
    println(f"${name} -- meta")
    printStat(name ++ " partitions", df.rdd.getNumPartitions)
    printStat(
      name ++ " is persisted",
      df.storageLevel.useMemory || df.storageLevel.useDisk
    )
    printStat(name ++ " use memory", df.storageLevel.useMemory)
    printStat(name ++ " use disk", df.storageLevel.useDisk)
  }

  def computeMonotonicTxId(block: Int, positionInBlock: Int): Long = {
    ((block.toLong) << 32) | (positionInBlock & 0xffffffffL);
  }

  def decomposeMontonicTxId(txId: Long): Tuple2[Int, Int] = {
    ((txId >> 32).toInt, (txId).toInt)
  }

  val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def printStat[R](name: String, value: R): Unit = {
    println(f"${name}%-40s: ${value}%40s")
  }

  def time[R](title: String)(block: => R): R = {
    val timestamp = LocalDateTime.now().format(dtf)
    println(s"Start [${timestamp}] - ${title}")

    val (r, elapsedTime) = time(block)

    val timestampDone = LocalDateTime.now().format(dtf)
    val hour = TimeUnit.MILLISECONDS.toHours(elapsedTime)
    val min = TimeUnit.MILLISECONDS.toMinutes(elapsedTime) % 60
    val sec = TimeUnit.MILLISECONDS.toSeconds(elapsedTime) % 60

    println(
      s"Done  [${timestampDone}] - ${title} took ${"%02d:%02d:%02d".format(hour, min, sec)}"
    )

    return r
  }

  def time[R](block: => R): Tuple2[R, Long] = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  def toIntSafe(value: Long): Int = {
    if (value.isValidInt) {
      return value.toInt
    } else {
      throw new ArithmeticException(f"${value} is out of an integers range.")
    }
  }

  def sha256(data: Array[Byte]): Array[Byte] = {
    MessageDigest
      .getInstance("SHA-256")
      .digest(data)
  }

  // https://idiomaticsoft.com/post/2023-05-23-base58/
  val base58_alphabet =
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

  val base58idxToChar = Map(base58_alphabet.zipWithIndex.map(_.swap): _*)
  // val base58charToIdx = Map(base58_alphabet.zipWithIndex: _*)

  def byteArrayToBigInt(a: Array[Byte]) =
    a.reverse.zipWithIndex.foldLeft(BigInt(0)) { (acc, b) =>
      val (digit, idx) = b
      acc + BigInt(256).pow(idx) * BigInt(java.lang.Byte.toUnsignedInt(digit))
    }

  def base58Convert(a: Array[Byte]) =
    baseConvert(byteArrayToBigInt(a), 58).map(base58idxToChar).mkString

  def baseConvert(n: BigInt, b: Int): Array[Int] = {
    if (n == 0) Array.emptyIntArray
    else {
      val m = n % b
      val q = n / b
      baseConvert(q, b) :+ m.toInt
    }
  }

  def encodeBase58(a: Array[Byte]) =
    a.takeWhile(_ == 0)
      .map(x => base58idxToChar(x.toInt))
      .mkString ++ base58Convert(
      a
    )

}
