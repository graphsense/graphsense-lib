TRON_DUMMY_REPLACEMENT_ADDRESS = (
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00ikna"
)

NULL_ADDRESS_ACCOUNT_BYTES = b"\x00" * 20


def replace_tron_dummy_address_with_valid_null_address(
    address: bytes, replace_empty: bool = False
) -> bytes:
    if address == TRON_DUMMY_REPLACEMENT_ADDRESS or (address == b"" and replace_empty):
        return NULL_ADDRESS_ACCOUNT_BYTES
    return address
