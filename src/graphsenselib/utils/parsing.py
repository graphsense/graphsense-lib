# flake8: noqa: E731
from parsy import regex, seq, string

space = regex(r"\s+")  # non-optional whitespace
padding = regex(r"\s*")  # optional whitespace
anything = regex(".*")
identifier = regex("[a-zA-Z][a-zA-Z0-9_]*")
tableidentifier = seq(keyspace=identifier << string("."), table=identifier) | seq(
    table=identifier
)

lexeme = lambda p: p << padding
ci_str_token = lambda str: lexeme(string(str, transform=lambda s: s.upper()))
