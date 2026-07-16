import logging

from graphsenselib.utils.cassandra import (
    DEFAULT_CASSANDRA_PORT,
    split_nodes_and_port,
)


def test_split_bare_hosts_uses_default_port():
    assert split_nodes_and_port(["node1", "node2"]) == (
        ["node1", "node2"],
        DEFAULT_CASSANDRA_PORT,
    )


def test_split_strips_port_from_every_node():
    assert split_nodes_and_port(["node1:9142", "node2:9142"]) == (
        ["node1", "node2"],
        9142,
    )


def test_split_mixed_nodes_takes_the_configured_port():
    assert split_nodes_and_port(["node1:9142", "node2"]) == (["node1", "node2"], 9142)


def test_split_empty_is_default_port():
    assert split_nodes_and_port([]) == ([], DEFAULT_CASSANDRA_PORT)


def test_split_conflicting_ports_takes_first_and_warns(caplog):
    with caplog.at_level(logging.WARNING):
        hosts, port = split_nodes_and_port(["a:9042", "b:9142"])

    assert (hosts, port) == (["a", "b"], 9042)
    assert "conflicting ports" in caplog.text


def test_split_accepts_an_iterator():
    # Callers pass config lists, but the helper must not depend on re-iterating.
    assert split_nodes_and_port(iter(["node1:9142"])) == (["node1"], 9142)
