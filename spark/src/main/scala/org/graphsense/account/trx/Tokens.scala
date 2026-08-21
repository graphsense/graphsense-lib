package org.graphsense.account.trx.tokens

import org.graphsense.account.Implicits._
import org.graphsense.account.TokenSet
import org.graphsense.account.models.TokenConfiguration

import math.pow

object TrxTokenSet extends TokenSet {

  private val supportedTokens = List(
    TokenConfiguration(
      "USDT",
      hexStrToBytes(
        "0xa614f803b6fd780986a42c78ec9c7f77e6ded13c"
      ), // TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t
      "trc20",
      6,
      pow(10, 6).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "USDC",
      hexStrToBytes(
        "0x3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe"
      ), // TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8
      "trc20",
      6,
      pow(10, 6).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "WTRX",
      hexStrToBytes(
        "0x891cdb91d149f23b1a45d9c5ca78a88d0cb44c18"
      ), // TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR
      "trc20",
      6,
      pow(10, 6).longValue(),
      Some("TRX")
    )
  )

  def getSupportedTokens(): List[TokenConfiguration] = {
    supportedTokens
  }

}
