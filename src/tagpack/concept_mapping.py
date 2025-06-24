import re
from functools import reduce
from typing import Iterable, Set

_concrete_concept_map = {
    "financial crime": "financial_crime",
    "file sharing": "filesharing",
    "child sexual abuse": "child_sexual_abuse",
    "money laundering": "money_laundering",
    "gambling": "gambling",
    "ponzi scheme": "ponzi_scheme",
    "violent crime": "violence",
    "index": "index",
    "wallet service": "wallet_service",
    "torture": "torture",
    "messaging service": "messaging_service",
    "private keys": "financial_crime",
    "hacked accounts": "account_hack",
    "sextortion": "sextortion",
    "extortion": "extortion",
    "mining": "mining_service",
    "exploits": "exploit",
    "scam": "scam",
    "market": "market",
    "hacking": "hacking",
    "search engine": "search_engine",
    "social engineering": "social_engineering",
    "leaked data": "data_breach",
    "mixing service": "mixing_service",
    "exchange": "exchange",
    "extremism": "extremism",
    "malware": "malware",
    "ddos": "hacking",
    "vpn provider": "vpn",
    "darkbat market": "market",
    "atm hacking": "hacking",
    "human trafficking": "human_trafficking",
    "single vendor shop (fraud)": "market",
    "fraud": "abuse",
    "hurtcore": "violence",
    "coin swap": "mining_service",
    "casino": "gambling",
    "under market 2.0": "market",
    "bank accounts": "financial_crime",
    "exit scam": "scam",
    "gore": "violence",
}

assert all(x.islower() for x in _concrete_concept_map.keys())

_regex_market_or_shop = r"market$|shop$"

_regex_drugs = r"weed|cannabis|fentanyl|heroin|drugs|ketamine|cocaine|cocain|methamphetamine|mdma|hard drugs"  # noqa

_regex_counterfeit = (
    r"fake id|drivers license|id cards|diploma|passport|certificates|counterfeit"
)

_regex_abuse = (
    r"humiliation|spam service|bullying|cybercrime|crime as a service|mistreat"
)

_regex_sex_abuse = r"sexual abuse|rape|animal sexual abuse"

_regex_payment_card_fraud = r"carding|credit cards|cloned cards|gift cards|skimming"

_regex_murder = r"murder|assasination|kill order|hitman"

_regex_weapons = r"kalashnikov|ammunition|arms|firearms|weapons|guns"

_regex_ransomeware = r"ransomeware$"

_regex_hosting = r"hosting"


def map_concepts_to_supported_concepts(foreign_concepts: Iterable[str]) -> Set[str]:
    return reduce(
        lambda x, y: x | y,
        (map_concept_to_supported_concepts(c) for c in foreign_concepts),
        set(),
    )


def map_concept_to_supported_concepts(foreign_concept: str) -> Set[str]:
    results = set()
    search_str = foreign_concept.lower().strip()
    if re.match(_regex_market_or_shop, search_str):
        results.add("market")

    if re.match(_regex_drugs, search_str):
        results.add("drugs")

    if re.match(_regex_counterfeit, search_str):
        results.add("counterfeit")

    if re.match(_regex_abuse, search_str):
        results.add("abuse")

    if re.match(_regex_sex_abuse, search_str):
        results.add("sexual_abuse")

    if re.match(_regex_payment_card_fraud, search_str):
        results.add("payment_card_fraud")

    if re.match(_regex_murder, search_str):
        results.add("murder")

    if re.match(_regex_weapons, search_str):
        results.add("weapons")

    if re.match(_regex_ransomeware, search_str):
        results.add("ransomeware")

    if re.match(_regex_hosting, search_str):
        results.add("hosting")

    cc = _concrete_concept_map.get(foreign_concept.lower(), None)

    if cc:
        results.add(cc)

    return results
