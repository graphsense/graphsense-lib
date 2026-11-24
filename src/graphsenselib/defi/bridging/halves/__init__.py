"""The half-bridge readers of the bridges an external backend pairs, one
module per bridge, named as in included_bridges. Each module's
``read(network, logs)`` returns the bridge's half bridges among a tx's logs
in log order (defi/bridging/delegated.py registers them)."""
