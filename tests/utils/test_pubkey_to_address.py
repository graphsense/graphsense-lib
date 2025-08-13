from graphsenselib.utils.pubkey_to_address import convert_pubkey_to_addresses


def test_pubkey_to_address():
    addresses = convert_pubkey_to_addresses(
        b"\x02\xc1\x10?c\xe1\xc3\x92\xba71\x17\xf9\xd0\xd0C\xe3U\xa3\x16G\x9f\xa5-\xf2\x86\xe3\x9f#\xd6\x00\xe0\xe6".hex()
    )

    assert addresses["btc"]["p2pkh"] == "1PCgFx6mHdyX12JzbJ914WBg578yzEbmV3"
    assert addresses["eth"]["eth"] == "0x67ad8d54aaa3a97b5dd0bcacf270970c16f9f36d"
    assert addresses["trx"]["trx"] == "TKRQWRic4gdv2ZuxFxTrLYT2K4W6wqqfAS"

    addresses = convert_pubkey_to_addresses(
        "027a4da4310322bd0d85d84b25cce883f2dbf119cfa74e80aa6555de765c003438"
    )
    assert addresses["btc"]["p2pkh"] == "145T2RPfvwbW7aKQ4gfqV8arx3Bhb3uWXZ"
    assert addresses["btc"]["p2pkh_uncomp"] == "1EubgrUEBq14jn1BYJxKf766v8ERNPhn9M"
    assert addresses["btc"]["p2sh_p2wpkh"] == "3B1VpWfpBVgevHbi6aPvEmCgHq9cykdWFw"
    assert (
        addresses["btc"]["p2wpkh_bech32"]
        == "bc1qyxlu8m0llp05yfdq9lm86sz9jvdxvfkgecq79y"
    )
    assert addresses["eth"]["eth"] == "0x1ed56fd00adf1bacd01701798b1ee9825ae523ee"
    assert addresses["ltc"]["p2pkh"] == "LNJQHdhW1bqZNP1ZEpf8m9edAFYykmiqYE"
    assert addresses["ltc"]["p2pkh_uncomp"] == "LZ8Yx4n4GVF7zahLiSwcw89s8LbhVj8SaJ"
    assert addresses["ltc"]["p2sh_p2wpkh"] == "MHDe8Q5n8cY5inscCTPG4QT5cXk4yDLwP5"
    assert (
        addresses["ltc"]["p2wpkh_bech32"]
        == "ltc1qyxlu8m0llp05yfdq9lm86sz9jvdxvfkgay66a5"
    )
    assert addresses["trx"]["trx"] == "TCnExfGMUozCmUFpBBMAAGkEVJFS4yTJwn"
