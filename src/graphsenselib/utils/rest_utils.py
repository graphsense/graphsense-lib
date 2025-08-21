import re


def get_first_key_present(target_dict, keylist):
    for k in keylist:
        if k in target_dict:
            return target_dict[k]
    raise KeyError(f"Non of the keys {keylist} is present in {target_dict}.")


def is_eth_like(network: str) -> bool:
    return network.upper() == "ETH" or network.upper() == "TRX"


def omit(d, keys):
    return {x: d[x] for x in d if x not in keys}


pattern = re.compile(r"[\W_]+", re.UNICODE)  # alphanumeric chars for label


def alphanumeric_lower(expression):
    return pattern.sub("", expression).lower()


def alphanumeric_lower_identifier(expression):
    split = expression.split("_")
    if len(split) == 1:
        return alphanumeric_lower(expression)
    else:
        return alphanumeric_lower(split[0]) + "_" + split[1]
