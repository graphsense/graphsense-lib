from parsy import regex, seq, string

space = regex(r"\s+")  # non-optional whitespace
padding = regex(r"\s*")  # optional whitespace
anything = regex(".*")
identifier = regex("[a-zA-Z][a-zA-Z0-9_]*")
tableidentifier = seq(keyspace=identifier << string("."), table=identifier) | seq(
    table=identifier
)


def lexeme(p):
    return p << padding


def ci_str_token(input_str):
    return lexeme(string(input_str, transform=lambda s: s.upper()))
