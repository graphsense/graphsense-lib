from cashaddress.convert import Address, InvalidAddress, to_legacy_address

# Patch P2SH32 legacy address support
# https://bch.info/en/upgrade
if ("P2SH32", 5, False) not in Address.VERSION_MAP["legacy"]:
    Address.VERSION_MAP["legacy"].append(("P2SH32", 5, False))
if ("P2SH32", 11, False) not in Address.VERSION_MAP["cash"]:
    Address.VERSION_MAP["cash"].append(("P2SH32", 11, False))


def bch_address_to_legacy(address):
    return to_legacy_address(address)


def try_bch_address_to_legacy(address):
    try:
        return bch_address_to_legacy(address)
    except InvalidAddress:
        return address
