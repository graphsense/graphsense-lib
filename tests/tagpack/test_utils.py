import pytest

from graphsenselib.tagpack.tagstore import TagStore
from graphsenselib.tagpack.utils import (
    get_github_repo_url,
    get_secondlevel_domain,
    normalize_tag_address,
)


def test_tld_extraction():
    assert get_secondlevel_domain("abc.co.uk") == "abc.co.uk"
    assert get_secondlevel_domain("spam.abc.co.uk") == "abc.co.uk"
    assert get_secondlevel_domain("spam.uk") == "spam.uk"
    assert get_secondlevel_domain("www.spam.uk") == "spam.uk"
    assert get_secondlevel_domain("www.spam.uk") == "spam.uk"
    assert get_secondlevel_domain("test.eth.link") == "test.eth.link"
    assert get_secondlevel_domain("foxbit.com.br") == "foxbit.com.br"
    assert get_secondlevel_domain("gardensdao.eth.limo") == "gardensdao.eth.limo"


@pytest.mark.parametrize("network", ["ETH", "USDT", "BSC", "TRX", ""])
def test_hex_address_lowercased_for_any_network(network):
    checksumaddr = "0xC61b9BB3A7a0767E3179713f3A5c7a9aeDCE193C"

    assert normalize_tag_address(checksumaddr, network) == checksumaddr.lower()


@pytest.mark.parametrize(
    "address,network",
    [
        ("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2", "BTC"),
        ("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "TRX"),
        ("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "USDT"),
        ("0xNotHexAtAll", "ETH"),
    ],
)
def test_non_hex_address_kept_verbatim(address, network):
    assert normalize_tag_address(address, network) == address


BCH_LEGACY = "1BpEi6DfDAUFd7GtittLSdBeYJvcoaVggu"  # CashAddr spec example


@pytest.mark.parametrize(
    "address",
    [
        "bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a",
        "BITCOINCASH:QPM2QSZNHKS23Z7629MMS6S4CWEF74VCWVY22GDX6A",
        # no prefix: was stored as typed, while REST lookups accept it
        "qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a",
        BCH_LEGACY,
    ],
)
@pytest.mark.parametrize("network", ["BCH", "bch"])
def test_bch_address_normalized_to_legacy(address, network):
    assert normalize_tag_address(address, network) == BCH_LEGACY


@pytest.mark.parametrize(
    "address",
    [
        "bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6b",  # checksum
        "qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6b",
    ],
)
def test_invalid_bch_cashaddr_kept_as_is(address):
    assert normalize_tag_address(address, "BCH") == address


@pytest.mark.parametrize(
    "uri, expected",
    [
        (
            "https://github.com/iknaio/tagpacks/blob/main/packs/x.yaml",
            "https://github.com/iknaio/tagpacks",
        ),
        ("github.com/iknaio/tagpacks/tree/main", "http://github.com/iknaio/tagpacks"),
        ("https://example.org/packs/x.yaml", None),
        (None, None),  # tagpack without uri (NULL in the tagstore)
        ("", None),
    ],
)
def test_get_github_repo_url(uri, expected):
    assert get_github_repo_url(uri) == expected


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, query):
        self.query = query

    def fetchall(self):
        return self.rows


def test_tagstore_source_repos_with_a_tagpack_without_uri():
    # crashed with AttributeError ('NoneType' has no 'startswith') before
    ts = TagStore.__new__(TagStore)  # no DB connection needed
    ts.cursor = _FakeCursor(
        [
            ("https://github.com/iknaio/a/blob/main/p1.yaml",),
            ("https://github.com/iknaio/a/blob/main/p2.yaml",),
            ("https://example.org/p3.yaml",),
            (None,),
        ]
    )
    repos = ts.tagstore_source_repos()
    assert sorted(repos, key=lambda r: str(r["uri"])) == [
        {"uri": None},
        {"uri": "https://example.org/p3.yaml"},
        {"uri": "https://github.com/iknaio/a"},
    ]
