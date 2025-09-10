import os
import pytest

from graphsenselib.utils import (
    batch,
    batch_date,
    bytes_to_hex,
    first_or_default,
    generate_date_range_days,
    remove_prefix,
    strip_0x,
    subkey_exists,
    subkey_get,
    to_int,
    truncateI32,
)
from graphsenselib.utils.errorhandling import CrashRecoverer
from graphsenselib.utils.generic import camel_to_snake_case, dict_to_dataobject


def test_dict_to_dataobject():
    d = {"a": 1}

    class Mock:
        a = 1

    obj2 = Mock()
    obj = dict_to_dataobject(d)

    assert obj.a == 1  # type: ignore
    assert obj2.a == 1
    assert dict_to_dataobject(obj2).a == 1  # type: ignore


def test_btoh_works(capsys):
    assert bytes_to_hex(b"") is None
    assert bytes_to_hex(b"asdfasdf") == "6173646661736466"


def test_strip_0x_works1():
    assert strip_0x("0xa9059cbb") == "a9059cbb"


def test_strip_0x_works2():
    assert strip_0x("a9059cbb") == "a9059cbb"
    assert strip_0x(None) is None


def test_to_int1():
    assert to_int(0) == 0
    assert to_int("1111") == 1111
    assert to_int("0xf") == 15
    assert to_int("0x10") == 16


def test_crash_recoverer():
    file = "/tmp/test_graphsense_lib_crashrecoverer.err"
    if os.path.exists(file):
        os.remove(file)
    cr = CrashRecoverer(file)

    try:
        with cr.enter_critical_section({"int": 1}):
            raise NotImplementedError("test")
    except NotImplementedError:
        assert cr.is_in_recovery_mode()
        assert cr.get_recovery_hint() == {
            "int": 1,
            "exception": "test",
            "exception_type": "NotImplementedError",
        }

    cr = CrashRecoverer(file)
    assert cr.is_in_recovery_mode()
    assert cr.get_recovery_hint() == {
        "int": 1,
        "exception": "test",
        "exception_type": "NotImplementedError",
    }

    try:
        with cr.enter_critical_section({}):
            pass
    except ValueError:
        pass

    assert cr.get_recovery_hint() == {
        "int": 1,
        "exception": "test",
        "exception_type": "NotImplementedError",
    }

    cr.leave_recovery_mode()
    assert not cr.is_in_recovery_mode()
    assert not os.path.exists(file)


def test_batch_works():
    assert [list(b) for b in batch(range(763638, 763639 + 1), n=1)] == [
        [763638],
        [763639],
    ]
    assert [list(b) for b in batch(range(763638, 763639 + 1), n=2)] == [
        [763638, 763639]
    ]
    assert [list(b) for b in batch(range(763638, 763640 + 1), n=2)] == [
        [763638, 763639],
        [763640],
    ]


def test_subkey_exists():
    assert subkey_exists({}, ["abc"]) is False
    assert subkey_exists({"abc": None}, ["abc"]) is True
    assert subkey_exists({"abc": []}, ["abc", "cbd"]) is False
    assert subkey_exists({"abc": {"cbd": 1}}, ["abc", "cbd"]) is True
    assert subkey_exists({"abc": {"cbd": None}}, ["abc", "cbd"]) is True


def test_subkey_get():
    assert subkey_get({}, ["abc"]) is None
    assert subkey_get({"abc": None}, ["abc"]) is None
    assert subkey_get({"abc": []}, ["abc", "cbd"]) is None
    assert subkey_get({"abc": {"cbd": 1}}, ["abc", "cbd"]) == 1
    assert subkey_get({"abc": {"cbd": "string"}}, ["abc", "cbd"]) == "string"
    assert (
        subkey_get({"abc": {"cbd": ["a", "b"], "bbb": [1, 2, 3]}}, ["abc", "cbd", "1"])
        == "b"
    )
    assert subkey_get(
        {"abc": {"cbd": ["a", "b"], "bbb": [1, 2, 3]}}, ["abc", "bbb"]
    ) == [1, 2, 3]


def test_first_or_default():
    assert first_or_default([1, 2, 3], lambda x: x > 2, default=10) == 3
    assert first_or_default([1, 2, 3], lambda x: x > 5, default=10) == 10
    assert first_or_default([1, 2, 3], lambda x: x > 5, default=None) is None


def test_remove_prefix():
    assert remove_prefix("0xa9059cbb", "0x") == "a9059cbb"
    assert remove_prefix("0xa9059cbb", "a0x") == "0xa9059cbb"


def test_date_range_works():
    from datetime import date

    a = date.fromisoformat("2010-03-12")
    b = date.fromisoformat("2024-05-19")

    batches = list(batch_date(a, b, days=180))

    deltas = [y - x for x, y in batches]

    assert all(d.days == 180 for d in deltas[:-1])

    assert batches[0][0] == a
    assert batches[-1][1] == b
    assert (sum(d.days for d in deltas) + len(deltas) - 1) == (b - a).days

    gen_dates = list(generate_date_range_days(a, b))

    assert gen_dates[0] == a
    assert gen_dates[-1] == b

    assert len(gen_dates) == (b - a).days + 1


def testTruncateI32():
    for i in range(0, 2147483647, 1000):
        assert truncateI32(i) == i

    assert truncateI32(2147483647) == 2147483647

    assert truncateI32(2147483648) == -2147483648


def test_simple_camel_case():
    """Test basic camelCase conversion."""
    assert camel_to_snake_case("camelCase") == "camel_case"
    assert camel_to_snake_case("firstName") == "first_name"
    assert camel_to_snake_case("lastName") == "last_name"


def test_pascal_case():
    """Test PascalCase conversion."""
    assert camel_to_snake_case("PascalCase") == "pascal_case"
    assert camel_to_snake_case("FirstName") == "first_name"
    assert camel_to_snake_case("XMLParser") == "xml_parser"


def test_multiple_uppercase_letters():
    """Test strings with consecutive uppercase letters."""
    assert camel_to_snake_case("XMLHttpRequest") == "xml_http_request"
    assert camel_to_snake_case("URLPath") == "url_path"
    assert camel_to_snake_case("HTTPSConnection") == "https_connection"
    assert camel_to_snake_case("HTMLElement") == "html_element"


def test_single_words():
    """Test single word strings."""
    assert camel_to_snake_case("word") == "word"
    assert camel_to_snake_case("Word") == "word"
    assert camel_to_snake_case("WORD") == "word"


def test_empty_and_edge_cases():
    """Test empty strings and edge cases."""
    assert camel_to_snake_case("") == ""
    assert camel_to_snake_case("a") == "a"
    assert camel_to_snake_case("A") == "a"


def test_already_snake_case():
    """Test strings that are already in snake_case."""
    assert camel_to_snake_case("snake_case") == "snake_case"
    assert camel_to_snake_case("already_snake") == "already_snake"
    assert camel_to_snake_case("_private_var") == "_private_var"


def test_mixed_formats():
    """Test strings with mixed formatting."""
    assert camel_to_snake_case("camelCase_mixed") == "camel_case_mixed"
    assert camel_to_snake_case("Mixed_camelCase") == "mixed_camel_case"
    assert camel_to_snake_case("someVarWith_underscores") == "some_var_with_underscores"


def test_numbers_in_strings():
    """Test strings containing numbers."""
    assert camel_to_snake_case("version2") == "version2"
    assert camel_to_snake_case("Version2") == "version2"
    assert camel_to_snake_case("html5Parser") == "html5_parser"
    assert camel_to_snake_case("get2FACode") == "get2_fa_code"


def test_special_characters():
    """Test strings with special characters."""
    assert camel_to_snake_case("camelCase@test") == "camel_case@test"
    assert camel_to_snake_case("someVar-withDash") == "some_var-with_dash"
    assert camel_to_snake_case("testVar.property") == "test_var.property"


def test_long_camel_case_strings():
    """Test longer, more complex camelCase strings."""
    assert (
        camel_to_snake_case("thisIsAVeryLongCamelCaseString")
        == "this_is_a_very_long_camel_case_string"
    )
    assert (
        camel_to_snake_case("getAddressTransactionsByNodeType")
        == "get_address_transactions_by_node_type"
    )
    assert (
        camel_to_snake_case("crossChainPubkeyRelatedAddress")
        == "cross_chain_pubkey_related_address"
    )


def test_common_programming_terms():
    """Test common programming terms and identifiers."""
    assert camel_to_snake_case("userId") == "user_id"
    assert camel_to_snake_case("sessionToken") == "session_token"
    assert camel_to_snake_case("apiEndpoint") == "api_endpoint"
    assert camel_to_snake_case("databaseConnection") == "database_connection"
    assert camel_to_snake_case("responseData") == "response_data"


def test_abbreviations():
    """Test handling of abbreviations and acronyms."""
    assert camel_to_snake_case("httpURL") == "http_url"
    assert camel_to_snake_case("jsonAPI") == "json_api"
    assert camel_to_snake_case("sqlDB") == "sql_db"
    assert camel_to_snake_case("cssStyle") == "css_style"


def test_single_uppercase_letters():
    """Test strings with single uppercase letters."""
    assert camel_to_snake_case("getA") == "get_a"
    assert camel_to_snake_case("setX") == "set_x"
    assert camel_to_snake_case("parseJSON") == "parse_json"


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("camelCase", "camel_case"),
        ("PascalCase", "pascal_case"),
        ("simple", "simple"),
        ("Simple", "simple"),
        ("XMLHttpRequest", "xml_http_request"),
        ("getHTTPSProxy", "get_https_proxy"),
        ("", ""),
        ("A", "a"),
        ("AB", "ab"),
        ("ABC", "abc"),
        ("someVariableName", "some_variable_name"),
        ("SomeClassName", "some_class_name"),
        ("version1Point2", "version1_point2"),
        ("HTML5Parser", "html5_parser"),
    ],
)
def test_parametrized_cases(input_str, expected):
    """Parametrized test cases for various input/output combinations."""
    assert camel_to_snake_case(input_str) == expected


def test_graphsense_specific_terms():
    """Test GraphSense-specific terminology conversions."""
    assert camel_to_snake_case("blockHeight") == "block_height"
    assert camel_to_snake_case("txHash") == "tx_hash"
    assert camel_to_snake_case("addressBalance") == "address_balance"
    assert camel_to_snake_case("crossChainMapping") == "cross_chain_mapping"
    assert camel_to_snake_case("tagstoreConfig") == "tagstore_config"
    assert camel_to_snake_case("cassandraConnection") == "cassandra_connection"
