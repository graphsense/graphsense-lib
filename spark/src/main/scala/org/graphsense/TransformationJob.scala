package org.graphsense

import org.graphsense.utxo.{TransformationJob => Utxo}
import org.graphsense.account.{TransformationJob => Account}
import org.rogach.scallop._

class CliArgs(arguments: Seq[String]) extends ScallopConf(arguments) {
  val network: ScallopOption[String] = opt[String](
    "network",
    required = true,
    noshort = true,
    descr =
      "Select which network we are processing (btc, zec, bch, ltc, eth, trx)"
  )
  verify()
}

object TransformationJob {

  def main(args: Array[String]): Unit = {

    /*
    extract only network arg, scallop does not ignore
    extra argument
     */

    val index = args.indexOf("network")
    val networkArgs = args.slice(index, index + 3)

    val conf = new CliArgs(networkArgs)

    conf.network().toLowerCase() match {
      case "eth" | "trx"                 => Account.main(args)
      case "btc" | "zec" | "bch" | "ltc" => Utxo.main(args)
      case _ =>
        throw new IllegalArgumentException(
          "Network " + conf.network() + " not supported."
        )
    }

  }

}
