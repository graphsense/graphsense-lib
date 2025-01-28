class UserFacingExceptions(Exception):
    """Hierarchy of exceptions that end up being communicated
    to the end user, but do not produce error logs"""


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
