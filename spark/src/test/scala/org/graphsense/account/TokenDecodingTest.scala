package org.graphsense.account

import org.graphsense.account.contract.tokens.Erc20Decoder
import org.graphsense.account.Implicits._
import org.graphsense.account.models.{Log, TokenTransfer}
import org.graphsense.TestBase

class DecodingTokenTest extends TestBase {

  test("test convertions") {
    val original = "0xdAC17F958D2ee523a2206206994597C13D831ec7".toLowerCase()
    val b = hexStrToBytes(original)
    assert(bytesToHexStr(b) == original)
  }

  test("test convertions 2") {
    val original =
      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        .toLowerCase()
    assert(bytesToHexStr(hexStrToBytes(original)) == original)
  }

  test("Default Transfer") {
    assert(TokenTransfer.default().isDefault)
  }

  test("decode log") {
    val l = Log(
      15000,
      15000000,
      "0x9a71a95be3fe957457b11817587e5af4c7e24836d5b383c430ff25b9286a457f",
      "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
      "0x0000000000000000000000000000000000000000000000005ea0a1fe55143c0d",
      Seq(
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x00000000000000000000000006729eb2424da47898f935267bd4a62940de5105",
        "0x000000000000000000000000beefbabeea323f07c59926295205d3b7a17e8638"
      ),
      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
      "0xbeb3d09a644dc0772719f498172af05ed8cd337aaf83c2b5aee43be34fcb9dfb",
      0,
      2
    )

    val e = TokenTransfer(
      15000000,
      2,
      0,
      "0xbeb3d09a644dc0772719f498172af05ed8cd337aaf83c2b5aee43be34fcb9dfb",
      "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
      "0x06729eb2424da47898f935267bd4a62940de5105",
      "0xbeefbabeea323f07c59926295205d3b7a17e8638",
      "5ea0a1fe55143c0d"
    )

    val t = Erc20Decoder.decodeTransfer(l)

    assert(t.get === e)
    assert(t.get.isDefault == false)

  }
}
