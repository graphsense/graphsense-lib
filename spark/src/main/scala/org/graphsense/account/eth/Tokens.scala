package org.graphsense.account.eth.tokens

import org.graphsense.account.Implicits._
import org.graphsense.account.TokenSet
import org.graphsense.account.models.TokenConfiguration

import math.pow

object EthTokenSet extends TokenSet {

  private val supportedTokens = List(
    TokenConfiguration(
      "USDT",
      hexStrToBytes("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
      "erc20",
      6,
      pow(10, 6).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "USDC",
      hexStrToBytes("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
      "erc20",
      6,
      pow(10, 6).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "WETH",
      hexStrToBytes("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
      "erc20",
      18,
      pow(10, 18).longValue(),
      Some("ETH")
    ),
    TokenConfiguration(
      "USDS",
      hexStrToBytes("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
      "erc20",
      18,
      pow(10, 18).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "stETH",
      hexStrToBytes("0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"),
      "erc20",
      18,
      pow(10, 18).longValue(),
      Some("ETH")
    ),
    TokenConfiguration(
      "wstETH",
      hexStrToBytes("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"),
      "erc20",
      18,
      pow(10, 18).longValue(),
      Some("ETH")
    ),
    TokenConfiguration(
      "sUSDS",
      hexStrToBytes("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"),
      "erc20",
      18,
      pow(10, 18).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "DEUR",
      hexStrToBytes("0xbA3f535bbCcCcA2A154b573Ca6c5A49BAAE0a3ea"),
      "erc20",
      18,
      pow(10, 18).longValue(),
      Some("EUR")
    )
    // TokenConfiguration(
    //   "WBTC",
    //   hexStrToBytes("0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"),
    //   "erc20",
    //   8,
    //   pow(10, 8).longValue(),
    //   Some("BTC")
    // ) -- BTC STABLE COINS NEED SOME WORK TO LOAD exchange rates properly
  )

  def getSupportedTokens(): List[TokenConfiguration] = {
    supportedTokens
  }

}
