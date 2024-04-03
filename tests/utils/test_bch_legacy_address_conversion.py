from graphsenselib.utils.bch import bch_address_to_legacy


def test_P2SH32():
    # converted with https://bch.info/en/tools/cashaddr
    assert (
        bch_address_to_legacy(
            "bitcoincash:pvhfadjrmc4adxl8payajshf34wauh4jlrfnpuyetfnzh5llrnh9w3q8y9qcp"
        )
        == "B5Fj9EniQGXUKCCx1dfm7XXARMf38wBgtjPTQv9uaR24eTrj3T"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:pvqljqfuvj0dqf4l6s9mradymsvmucwe8w7cm84yyt8p2dtwj93xcuqwaelt6"
        )
        == "AjbGjygbRL7MwFc3PJ9KWVqMdUGeHsFxXDaiGzyTqQnBKP4iWo"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:pv4dcn3jppealt8y9t6egv4aj8snhtt4qqz006u27dezdwj65xkxuqaqmccfz"
        )
        == "B3bhUDrC913vH9rRaxnhToG4fi1hp8B55Sg1RHGE4AdCRoti1o"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:p0kfgkjy9rcm83esghxyajwkkteyemlnq9haaqgz4rwf7dcytj0a6y3cmcxvh"
        )
        == "CWuzJNZkUySucj86MBcUt6yjkuoMJeyYQbd1CiWfhMP6EZSSeJ"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:pwdk0fd8jvknxcjedn8z09h4g4h68ka8vhvld5gjpucv6p2qsa4s62md8hdms"
        )
        == "BuAVUU8EJ6X1ve8t3zNw4vQY1SexXMbonUU3UD476VzNkEtjzx"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:p0nexse24wql60lf6y243whrc2rtlqcvvex0yqcq48t04p6u0l9xjmrd85vuw"
        )
        == "CUiAL6mpQDyKoZRDh3tkChPs4A2oPiRUxqT6MQaQ7s5t7ZjrxY"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:pvmxavxc4s0af8dtuvwgt75jrv0hfy9dr6pnr4q3hqu5df2xsmx3shj35fhqy"
        )
        == "B8hHVPg5bZTfeC3xFsYqqJRge8GRzN53QRyhFQ1iCnxqi6EVvZ"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:p09zv5j9dyqcqpmvxchtvzvv3xx936g7ltg9re3mzmuw4pc9tenh2n7yflqf6"
        )
        == "CFkXRvGkzkgLVrcSFM7LDLXcQAUmc3qxtSsDdbqUyKDjd7cnhi"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:pwzvpampue7zmqgyc7dd9j0v8hawk2cpa5n3dqlcyq52y995qmgqx894n48m0"
        )
        == "BjBu9SQMT1TKcL2YANFzsV1JXYe3A4nbK7LWykidjcDhe4p5h6"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:pv7khk9tyvylvxgapldky8p700wt7yevwqzd2ucultnzqzphcflvwcjzn40nk"
        )
        == "BBmokgVUDC3RcuzwgYFR14fXwNqo2UGRfKt5Qr8YKGtW3zTsRw"
    )
    assert (
        bch_address_to_legacy(
            "bitcoincash:pdh7plp7yn4yhv2tnx7kc5jkvy7kgvj7w4wd5ma0epyk2h4zy4px7gnjhl04p"
        )
        == "BZzgGkgRk9idEyyr3Nrjx3S1t7yyMrFsWJfb2cD7jbo2qeV2q3"
    )


def test_Other():
    # taken from
    # https://github.com/bitcoincashorg/bitcoincash.org/blob/master/spec/cashaddr.md#examples-of-address-translation

    assert (
        bch_address_to_legacy("bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a")
        == "1BpEi6DfDAUFd7GtittLSdBeYJvcoaVggu"
    )
    assert (
        bch_address_to_legacy("bitcoincash:qr95sy3j9xwd2ap32xkykttr4cvcu7as4y0qverfuy")
        == "1KXrWXciRDZUpQwQmuM1DbwsKDLYAYsVLR"
    )
    assert (
        bch_address_to_legacy("bitcoincash:qqq3728yw0y47sqn6l2na30mcw6zm78dzqre909m2r")
        == "16w1D5WRVKJuZUsSRzdLp9w3YGcgoxDXb"
    )
    assert (
        bch_address_to_legacy("bitcoincash:ppm2qsznhks23z7629mms6s4cwef74vcwvn0h829pq")
        == "3CWFddi6m4ndiGyKqzYvsFYagqDLPVMTzC"
    )
    assert (
        bch_address_to_legacy("bitcoincash:pr95sy3j9xwd2ap32xkykttr4cvcu7as4yc93ky28e")
        == "3LDsS579y7sruadqu11beEJoTjdFiFCdX4"
    )
    assert (
        bch_address_to_legacy("bitcoincash:pqq3728yw0y47sqn6l2na30mcw6zm78dzq5ucqzc37")
        == "31nwvkZwyPdgzjBJZXfDmSWsC4ZLKpYyUw"
    )

    assert (
        bch_address_to_legacy("122c9XmGjeMcirALbBP1g2kh9N3uZZofbR")
        == "122c9XmGjeMcirALbBP1g2kh9N3uZZofbR"
    )

    # taken from
    # https://github.com/bitcoincashorg/bitcoincash.org/blob/master/spec/cashaddr.md#larger-test-vectors
    # currently addresses with payload > 160bit are not supported by cashaddress 1.0.6

    # assert bch_address_to_legacy('bitcoincash:qr6m7j9njldwwzlg9v7v53unlr4jkmx6eylep8ekg2') == 'F5BF48B397DAE70BE82B3CCA4793F8EB2B6CDAC9' # noqa
    # assert bch_address_to_legacy('bchtest:pr6m7j9njldwwzlg9v7v53unlr4jkmx6eyvwc0uz5t') == 'F5BF48B397DAE70BE82B3CCA4793F8EB2B6CDAC9' # noqa
    # assert bch_address_to_legacy('pref:pr6m7j9njldwwzlg9v7v53unlr4jkmx6ey65nvtks5') == 'F5BF48B397DAE70BE82B3CCA4793F8EB2B6CDAC9' # noqa
    # assert bch_address_to_legacy('prefix:0r6m7j9njldwwzlg9v7v53unlr4jkmx6ey3qnjwsrf') == 'F5BF48B397DAE70BE82B3CCA4793F8EB2B6CDAC9' # noqa
    # assert bch_address_to_legacy('bitcoincash:q9adhakpwzztepkpwp5z0dq62m6u5v5xtyj7j3h2ws4mr9g0') == '7ADBF6C17084BC86C1706827B41A56F5CA32865925E946EA' # noqa
    # assert bch_address_to_legacy('bchtest:p9adhakpwzztepkpwp5z0dq62m6u5v5xtyj7j3h2u94tsynr') == '7ADBF6C17084BC86C1706827B41A56F5CA32865925E946EA' # noqa
    # assert bch_address_to_legacy('pref:p9adhakpwzztepkpwp5z0dq62m6u5v5xtyj7j3h2khlwwk5v') == '7ADBF6C17084BC86C1706827B41A56F5CA32865925E946EA' # noqa
    # assert bch_address_to_legacy('prefix:09adhakpwzztepkpwp5z0dq62m6u5v5xtyj7j3h2p29kc2lp') == '7ADBF6C17084BC86C1706827B41A56F5CA32865925E946EA' # noqa
    # assert bch_address_to_legacy('bitcoincash:qgagf7w02x4wnz3mkwnchut2vxphjzccwxgjvvjmlsxqwkcw59jxxuz') == '3A84F9CF51AAE98A3BB3A78BF16A6183790B18719126325BFC0C075B' # noqa
    # assert bch_address_to_legacy('bchtest:pgagf7w02x4wnz3mkwnchut2vxphjzccwxgjvvjmlsxqwkcvs7md7wt') == '3A84F9CF51AAE98A3BB3A78BF16A6183790B18719126325BFC0C075B' # noqa
    # assert bch_address_to_legacy('pref:pgagf7w02x4wnz3mkwnchut2vxphjzccwxgjvvjmlsxqwkcrsr6gzkn') == '3A84F9CF51AAE98A3BB3A78BF16A6183790B18719126325BFC0C075B' # noqa
    # assert bch_address_to_legacy('prefix:0gagf7w02x4wnz3mkwnchut2vxphjzccwxgjvvjmlsxqwkc5djw8s9g') == '3A84F9CF51AAE98A3BB3A78BF16A6183790B18719126325BFC0C075B' # noqa
    # assert bch_address_to_legacy('bitcoincash:qvch8mmxy0rtfrlarg7ucrxxfzds5pamg73h7370aa87d80gyhqxq5nlegake') == '3173EF6623C6B48FFD1A3DCC0CC6489B0A07BB47A37F47CFEF4FE69DE825C060' # noqa
    # assert bch_address_to_legacy('bchtest:pvch8mmxy0rtfrlarg7ucrxxfzds5pamg73h7370aa87d80gyhqxq7fqng6m6') == '3173EF6623C6B48FFD1A3DCC0CC6489B0A07BB47A37F47CFEF4FE69DE825C060' # noqa
    # assert bch_address_to_legacy('pref:pvch8mmxy0rtfrlarg7ucrxxfzds5pamg73h7370aa87d80gyhqxq4k9m7qf9') == '3173EF6623C6B48FFD1A3DCC0CC6489B0A07BB47A37F47CFEF4FE69DE825C060' # noqa
    # assert bch_address_to_legacy('prefix:0vch8mmxy0rtfrlarg7ucrxxfzds5pamg73h7370aa87d80gyhqxqsh6jgp6w') == '3173EF6623C6B48FFD1A3DCC0CC6489B0A07BB47A37F47CFEF4FE69DE825C060' # noqa
    # assert bch_address_to_legacy('bitcoincash:qnq8zwpj8cq05n7pytfmskuk9r4gzzel8qtsvwz79zdskftrzxtar994cgutavfklv39gr3uvz') == 'C07138323E00FA4FC122D3B85B9628EA810B3F381706385E289B0B25631197D194B5C238BEB136FB' # noqa
    # assert bch_address_to_legacy('bchtest:pnq8zwpj8cq05n7pytfmskuk9r4gzzel8qtsvwz79zdskftrzxtar994cgutavfklvmgm6ynej') == 'C07138323E00FA4FC122D3B85B9628EA810B3F381706385E289B0B25631197D194B5C238BEB136FB' # noqa
    # assert bch_address_to_legacy('pref:pnq8zwpj8cq05n7pytfmskuk9r4gzzel8qtsvwz79zdskftrzxtar994cgutavfklv0vx5z0w3') == 'C07138323E00FA4FC122D3B85B9628EA810B3F381706385E289B0B25631197D194B5C238BEB136FB' # noqa
    # assert bch_address_to_legacy('prefix:0nq8zwpj8cq05n7pytfmskuk9r4gzzel8qtsvwz79zdskftrzxtar994cgutavfklvwsvctzqy') == 'C07138323E00FA4FC122D3B85B9628EA810B3F381706385E289B0B25631197D194B5C238BEB136FB' # noqa
    # assert bch_address_to_legacy('bitcoincash:qh3krj5607v3qlqh5c3wq3lrw3wnuxw0sp8dv0zugrrt5a3kj6ucysfz8kxwv2k53krr7n933jfsunqex2w82sl') == 'E361CA9A7F99107C17A622E047E3745D3E19CF804ED63C5C40C6BA763696B98241223D8CE62AD48D863F4CB18C930E4C' # noqa
    # assert bch_address_to_legacy('bchtest:ph3krj5607v3qlqh5c3wq3lrw3wnuxw0sp8dv0zugrrt5a3kj6ucysfz8kxwv2k53krr7n933jfsunqnzf7mt6x') == 'E361CA9A7F99107C17A622E047E3745D3E19CF804ED63C5C40C6BA763696B98241223D8CE62AD48D863F4CB18C930E4C' # noqa
    # assert bch_address_to_legacy('pref:ph3krj5607v3qlqh5c3wq3lrw3wnuxw0sp8dv0zugrrt5a3kj6ucysfz8kxwv2k53krr7n933jfsunqjntdfcwg') == 'E361CA9A7F99107C17A622E047E3745D3E19CF804ED63C5C40C6BA763696B98241223D8CE62AD48D863F4CB18C930E4C' # noqa
    # assert bch_address_to_legacy('prefix:0h3krj5607v3qlqh5c3wq3lrw3wnuxw0sp8dv0zugrrt5a3kj6ucysfz8kxwv2k53krr7n933jfsunqakcssnmn') == 'E361CA9A7F99107C17A622E047E3745D3E19CF804ED63C5C40C6BA763696B98241223D8CE62AD48D863F4CB18C930E4C' # noqa
    # assert bch_address_to_legacy('bitcoincash:qmvl5lzvdm6km38lgga64ek5jhdl7e3aqd9895wu04fvhlnare5937w4ywkq57juxsrhvw8ym5d8qx7sz7zz0zvcypqscw8jd03f') == 'D9FA7C4C6EF56DC4FF423BAAE6D495DBFF663D034A72D1DC7D52CBFE7D1E6858F9D523AC0A7A5C34077638E4DD1A701BD017842789982041' # noqa
    # assert bch_address_to_legacy('bchtest:pmvl5lzvdm6km38lgga64ek5jhdl7e3aqd9895wu04fvhlnare5937w4ywkq57juxsrhvw8ym5d8qx7sz7zz0zvcypqs6kgdsg2g') == 'D9FA7C4C6EF56DC4FF423BAAE6D495DBFF663D034A72D1DC7D52CBFE7D1E6858F9D523AC0A7A5C34077638E4DD1A701BD017842789982041' # noqa
    # assert bch_address_to_legacy('pref:pmvl5lzvdm6km38lgga64ek5jhdl7e3aqd9895wu04fvhlnare5937w4ywkq57juxsrhvw8ym5d8qx7sz7zz0zvcypqsammyqffl') == 'D9FA7C4C6EF56DC4FF423BAAE6D495DBFF663D034A72D1DC7D52CBFE7D1E6858F9D523AC0A7A5C34077638E4DD1A701BD017842789982041' # noqa
    # assert bch_address_to_legacy('prefix:0mvl5lzvdm6km38lgga64ek5jhdl7e3aqd9895wu04fvhlnare5937w4ywkq57juxsrhvw8ym5d8qx7sz7zz0zvcypqsgjrqpnw8') == 'D9FA7C4C6EF56DC4FF423BAAE6D495DBFF663D034A72D1DC7D52CBFE7D1E6858F9D523AC0A7A5C34077638E4DD1A701BD017842789982041' # noqa
    # assert bch_address_to_legacy('bitcoincash:qlg0x333p4238k0qrc5ej7rzfw5g8e4a4r6vvzyrcy8j3s5k0en7calvclhw46hudk5flttj6ydvjc0pv3nchp52amk97tqa5zygg96mtky5sv5w') == 'D0F346310D5513D9E01E299978624BA883E6BDA8F4C60883C10F28C2967E67EC77ECC7EEEAEAFC6DA89FAD72D11AC961E164678B868AEEEC5F2C1DA08884175B' # noqa
    # assert bch_address_to_legacy('bchtest:plg0x333p4238k0qrc5ej7rzfw5g8e4a4r6vvzyrcy8j3s5k0en7calvclhw46hudk5flttj6ydvjc0pv3nchp52amk97tqa5zygg96mc773cwez') == 'D0F346310D5513D9E01E299978624BA883E6BDA8F4C60883C10F28C2967E67EC77ECC7EEEAEAFC6DA89FAD72D11AC961E164678B868AEEEC5F2C1DA08884175B' # noqa
    # assert bch_address_to_legacy('pref:plg0x333p4238k0qrc5ej7rzfw5g8e4a4r6vvzyrcy8j3s5k0en7calvclhw46hudk5flttj6ydvjc0pv3nchp52amk97tqa5zygg96mg7pj3lh8') == 'D0F346310D5513D9E01E299978624BA883E6BDA8F4C60883C10F28C2967E67EC77ECC7EEEAEAFC6DA89FAD72D11AC961E164678B868AEEEC5F2C1DA08884175B' # noqa
    # assert bch_address_to_legacy('prefix:0lg0x333p4238k0qrc5ej7rzfw5g8e4a4r6vvzyrcy8j3s5k0en7calvclhw46hudk5flttj6ydvjc0pv3nchp52amk97tqa5zygg96ms92w6845') == 'D0F346310D5513D9E01E299978624BA883E6BDA8F4C60883C10F28C2967E67EC77ECC7EEEAEAFC6DA89FAD72D11AC961E164678B868AEEEC5F2C1DA08884175B' # noqa
