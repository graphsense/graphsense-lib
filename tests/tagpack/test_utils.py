import pytest

from graphsenselib.tagpack.utils import get_secondlevel_domain, normalize_tag_address


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
