from graphsenselib.utils.address import address_to_user_format
from graphsenselib.datatypes import NodeType


class BadConfigError(Exception):
    """Raise for incorrect configuration objects"""


class DBInconsistencyException(Exception):
    """Raise this exception if an inconsistency in the DB occurred"""


class UserFacingExceptions(Exception):
    """Hierarchy of exceptions that end up being communicated
    to the end user, but do not produce error logs"""

    def __init__(self, user_msg: str):
        super().__init__(user_msg)
        self.user_msg = user_msg

    def get_user_msg(self):
        return self.user_msg


class GsTimeoutException(UserFacingExceptions):
    """Raise this exception if a request times out."""

    pass


class NotFoundException(UserFacingExceptions):
    """this exception should be used if some
    item is not found e.g. the database."""


class NetworkNotFoundException(NotFoundException):
    def __init__(self, network):
        super().__init__(f"Network {network} not supported")


class BlockNotFoundException(NotFoundException):
    def __init__(self, network, height):
        super().__init__(f"Block {height} not found in network {network}")


class TransactionNotFoundException(NotFoundException):
    def __init__(self, network, tx_hash, token_id=None):
        msg = (
            (f"Token transaction {tx_hash}:{token_id} in network {network} not found")
            if token_id
            else f"Transaction {tx_hash} not found in network {network}"
        )
        super().__init__(msg)


def nodeNotFoundException(network, node_type: NodeType, id):
    if node_type == NodeType.ADDRESS:
        return AddressNotFoundException(network, id)
    else:
        return ClusterNotFoundException(network, id)


class AddressNotFoundException(NotFoundException):
    def __init__(self, network, address, no_external_txs=False):
        address = address_to_user_format(network, address)
        reason = " because it has no external transactions" if no_external_txs else ""

        super().__init__(f"Address {address} not found in network {network}{reason}")


class ClusterNotFoundException(NotFoundException):
    def __init__(self, network, cluster):
        super().__init__(f"Cluster {cluster} not found in network {network}")


class BadUserInputException(UserFacingExceptions):
    """this exception should be used if the user input is not valid."""


class FeatureNotAvailableException(UserFacingExceptions):
    """this exception should be used if a requested feature is not available in the current instance."""
