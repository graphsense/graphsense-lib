package org.graphsense.models

// generic transform datatypes
case class ExchangeRatesRaw(
    date: String,
    fiatValues: Option[Map[String, Float]]
)

case class ExchangeRates(blockId: Int, fiatValues: Seq[Float])

case class TokenExchangeRatesRaw(
    asset: String,
    date: String,
    fiatValues: Option[Map[String, Float]]
)

case class TokenExchangeRates(
    asset: String,
    blockId: Int,
    fiatValues: Seq[Float]
)
