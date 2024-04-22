from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generator, Tuple

from ..config import GRAPHSENSE_DEFAULT_DATETIME_FORMAT
from ..schema.resources.parquet.account import BINARY_COL_CONVERSION_MAP_ACCOUNT
from ..schema.resources.parquet.account_trx import BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX
from ..utils import parse_timestamp
from ..utils.signals import graceful_ctlc_shutdown
from .account import (
    BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LEN,
    WEB3_QUERY_BATCH_SIZE,
    WEB3_QUERY_WORKERS,
    EthStreamerAdapter,
    TronStreamerAdapter,
    enrich_transactions,
    enrich_transactions_with_type,
    get_connection_from_url,
    get_last_synced_block,
    logger,
    prepare_blocks_inplace_eth,
    prepare_blocks_inplace_trx,
    prepare_fees_inplace,
    prepare_logs_inplace,
    prepare_traces_inplace_eth,
    prepare_traces_inplace_trx,
    prepare_transactions_inplace_eth,
    prepare_transactions_inplace_trx,
    prepare_trc10_tokens_inplace,
    to_bytes,
)
from .parquet import BlockRangeContent
from .utxo import (
    TX_HASH_PREFIX_LENGTH,
    enrich_txs,
    flatten,
    get_stream_adapter,
    get_tx_refs,
    prepare_blocks_inplace,
    prepare_refs_inplace_parquet,
    prepare_transactions_inplace_parquet,
)


def split_blockrange(
    blockrange: Tuple[int, int], size: int
) -> Generator[Tuple[int, int], None, None]:
    current = blockrange[0]
    while current <= blockrange[1]:
        yield (current, min(current + size - 1, blockrange[1]))
        current += size


class Source(ABC):
    @abstractmethod
    def read_blockrange(self, start_block, end_block) -> BlockRangeContent:
        pass

    @abstractmethod
    def read_blockindep(self) -> BlockRangeContent:
        pass

    @abstractmethod
    def get_last_synced_block(self) -> int:
        pass

    def validate_blockrange(self, start_block: int, end_block: int) -> Tuple[int, int]:
        assert start_block >= 0, "Start block must be greater or equal to 0"
        assert (
            start_block <= end_block
        ), "Start block must be less or equal to end block"
        assert start_block >= 0, "Start block must be greater or equal to 0"
        last_synced_block = self.get_last_synced_block()
        if end_block is None:
            end_block = last_synced_block
        logger.info(f"Last synced block: {end_block:,}")

        assert end_block >= 0, "End block must be greater or equal to 0"

        if end_block > last_synced_block:
            logger.warning(
                f"End block {end_block:,} is greater than last synced block "
                f"{last_synced_block:,}, setting end block to last synced block"
            )
            end_block = last_synced_block

        logger.info(
            f"Validated block range "
            f"{start_block:,} - {end_block:,} ({end_block - start_block + 1:,} blks) "
        )

        return start_block, end_block


class Transformer(ABC):
    def __init__(self, partition_batch_size: int, network: str):
        self.partition_batch_size = partition_batch_size
        self.network = network

    @abstractmethod
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        pass

    @abstractmethod
    def transform_blockindep(
        self, block_range_content: BlockRangeContent
    ) -> BlockRangeContent:
        pass


class Sink(ABC):
    @abstractmethod
    def write(self, block_range_content: BlockRangeContent):
        pass


class SourceTRX(Source):
    def __init__(
        self, provider_uri, grpc_provider_uri, provider_timeout, partition_batch_size
    ):
        self.provider_uri = provider_uri
        self.grpc_provider_uri = grpc_provider_uri
        self.provider_timeout = provider_timeout
        self.thread_proxy = get_connection_from_url(provider_uri, provider_timeout)
        self.adapter = TronStreamerAdapter(
            self.thread_proxy,
            grpc_endpoint=grpc_provider_uri,
            batch_size=WEB3_QUERY_BATCH_SIZE,
            max_workers=WEB3_QUERY_WORKERS,
        )

        self.partition_batch_size = partition_batch_size
        # todo should be able to remove this if the block transform is moved
        #  out of source
        # todo in order to do this, "export_hash_to_type_mappings" has to be
        #  adapter to pre-transformation naming

    def read_blockrange(self, start_block, end_block):
        blocks, txs = self.adapter.export_blocks_and_transactions(
            start_block, end_block
        )
        receipts, logs = self.adapter.export_receipts_and_logs(txs)
        traces, fees = self.adapter.export_traces(start_block, end_block, True, True)
        prepare_blocks_inplace_trx(
            blocks, BLOCK_BUCKET_SIZE, self.partition_batch_size
        )  # todo should be in transformer
        hash_to_type = self.adapter.export_hash_to_type_mappings(txs, blocks)

        data = {
            "blocks": blocks,
            "txs": txs,
            "receipts": receipts,
            "logs": logs,
            "traces": traces,
            "fees": fees,
            "hash_to_type": hash_to_type,
        }

        return BlockRangeContent(
            table_contents=data, start_block=start_block, end_block=end_block
        )

    def read_blockindep(self):
        token_infos = self.adapter.get_trc10_token_infos()
        return BlockRangeContent(
            table_contents={"token_infos": token_infos}
        )  # todo dirty, its not for a blockrange, but it also works

    def get_last_synced_block(self):
        return get_last_synced_block(self.thread_proxy)


class SourceETH(Source):
    def __init__(self, provider_uri, provider_timeout, partition_batch_size):
        self.provider_uri = provider_uri
        self.provider_timeout = provider_timeout
        self.thread_proxy = get_connection_from_url(provider_uri, provider_timeout)
        self.adapter = EthStreamerAdapter(
            self.thread_proxy,
            batch_size=WEB3_QUERY_BATCH_SIZE,
            max_workers=WEB3_QUERY_WORKERS,
        )

        self.partition_batch_size = partition_batch_size  # todo should be able
        # to remove this if the block transform is moved out of source
        # todo in order to do this, "export_hash_to_type_mappings"
        #  has to be adapter to pre-transformation naming

    def read_blockrange(self, start_block, end_block):
        blocks, txs = self.adapter.export_blocks_and_transactions(
            start_block, end_block
        )
        receipts, logs = self.adapter.export_receipts_and_logs(txs)
        traces, _ = self.adapter.export_traces(start_block, end_block, True, True)

        data = {
            "blocks": blocks,
            "txs": txs,
            "receipts": receipts,
            "logs": logs,
            "traces": traces,
        }

        return BlockRangeContent(
            table_contents=data, start_block=start_block, end_block=end_block
        )

    def read_blockindep(self):
        return BlockRangeContent(table_contents={})

    def get_last_synced_block(self):
        return get_last_synced_block(self.thread_proxy)


class SourceUTXO(Source):
    def __init__(self, provider_uri, network):
        self.adapter = get_stream_adapter(network, provider_uri, batch_size=30)

    def read_blockrange(self, start_block, end_block):
        blocks, txs = self.adapter.export_blocks_and_transactions(
            start_block, end_block
        )
        data = {"blocks": blocks, "txs": txs}

        return BlockRangeContent(
            table_contents=data, start_block=start_block, end_block=end_block
        )

    def read_blockindep(self):
        return BlockRangeContent(table_contents={})

    def get_last_synced_block(self):
        return self.adapter.get_current_block_number()


class TransformerUTXO(Transformer):
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]

        tx_refs = flatten(
            [get_tx_refs(tx["hash"], tx["inputs"], TX_HASH_PREFIX_LENGTH) for tx in txs]
        )

        prepare_blocks_inplace(
            blocks, BLOCK_BUCKET_SIZE, process_fields=False, drop_fields=False
        )

        # until bitcoin-etl progresses
        # with https://github.com/blockchain-etl/bitcoin-etl/issues/43
        enrich_txs(
            txs,
            resolver=None,
            ignore_missing_outputs=True,
            input_reference_only=True,
        )

        prepare_transactions_inplace_parquet(txs, self.network)
        prepare_refs_inplace_parquet(tx_refs)

        partition = (
            blocks[0]["block_id"] // self.partition_batch_size
        )  # todo this can be a problem if the there are multiple partitions

        def with_partition(items: list) -> list:
            for item in items:
                item["partition"] = partition
            return items

        block_range_content.table_contents = {
            "block": with_partition(blocks),
            "transaction": with_partition(txs),
            "transaction_spending": with_partition(tx_refs),
        }
        return block_range_content

    def transform_blockindep(self, data):
        return BlockRangeContent(table_contents={})


class TransformerTRX(Transformer):
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]
        receipts = data["receipts"]
        logs = data["logs"]
        traces = data["traces"]
        fees = data["fees"]
        hash_to_type = data["hash_to_type"]

        partition = (
            blocks[0]["block_id"] // self.partition_batch_size
        )  # todo this can be a problem if the there are multiple partitions
        # for an import range. probably best to add partition col another way

        # TRX ONLY
        txs = enrich_transactions(txs, receipts)
        txs = enrich_transactions_with_type(txs, hash_to_type)
        prepare_fees_inplace(fees, TX_HASH_PREFIX_LEN, partition)
        prepare_transactions_inplace = prepare_transactions_inplace_trx
        prepare_traces_inplace = prepare_traces_inplace_trx

        # COMMON
        prepare_transactions_inplace(
            txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE, self.partition_batch_size
        )
        prepare_traces_inplace(traces, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE, self.partition_batch_size)

        txs = to_bytes(txs, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["transaction"])
        blocks = to_bytes(blocks, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["block"])
        traces = to_bytes(traces, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["trace"])
        logs = to_bytes(logs, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["log"])

        block_range_content.table_contents = {
            "block": blocks,
            "transaction": txs,
            "log": logs,
            "trace": traces,
            "fee": fees,
        }
        return block_range_content

    def transform_blockindep(
        self, block_range_content: BlockRangeContent
    ) -> BlockRangeContent:
        token_infos = block_range_content.table_contents["token_infos"]
        prepare_trc10_tokens_inplace(token_infos)
        return BlockRangeContent(
            table_contents={"trc10": token_infos}
        )  # todo dirty, its not for a blockrange, but it also works


class TransformerETH(Transformer):
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]
        receipts = data["receipts"]
        logs = data["logs"]
        traces = data["traces"]

        prepare_blocks_inplace_eth(blocks, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        txs = enrich_transactions(txs, receipts)
        prepare_transactions_inplace_eth(
            txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE, self.partition_batch_size
        )
        prepare_traces_inplace_eth(traces, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE, self.partition_batch_size)

        txs = to_bytes(txs, BINARY_COL_CONVERSION_MAP_ACCOUNT["transaction"])
        blocks = to_bytes(blocks, BINARY_COL_CONVERSION_MAP_ACCOUNT["block"])
        traces = to_bytes(traces, BINARY_COL_CONVERSION_MAP_ACCOUNT["trace"])
        logs = to_bytes(logs, BINARY_COL_CONVERSION_MAP_ACCOUNT["log"])

        block_range_content.table_contents = {
            "block": blocks,
            "transaction": txs,
            "log": logs,
            "trace": traces,
        }
        return block_range_content

    def transform_blockindep(self, data):
        return BlockRangeContent(table_contents={})


class ETLRunner:
    def __init__(self, partition_batch_size: int, file_batch_size: int):
        self.source = None
        self.transformers = []
        self.sinks = []
        self.partition_batch_size = partition_batch_size
        self.file_batch_size = file_batch_size

    def addSource(self, source: Source):
        self.source = source

    def addTransformer(self, transformer: Transformer):
        self.transformers.append(transformer)

    def addSink(self, sink: Sink):
        self.sinks.append(sink)

    def ingest_range(self, start_block, end_block):
        source = self.source
        data = source.read_blockrange(start_block, end_block)

        for transformer in self.transformers:
            data = transformer.transform(data)

        for sink in self.sinks:
            sink.write(data)

        return data

    def ingest_blockindep(self):
        source = self.source
        data = source.read_blockindep()

        for transformer in self.transformers:
            data = transformer.transform_blockindep(data)

        for sink in self.sinks:
            sink.write(data)

        return data

    def run(self, start_block, end_block):
        start_block, end_block = self.source.validate_blockrange(start_block, end_block)

        partitions = split_blockrange(
            (start_block, end_block), self.partition_batch_size
        )

        with graceful_ctlc_shutdown() as check_shutdown_initialized:
            for partition in partitions:
                file_chunks = split_blockrange(partition, self.file_batch_size)
                for file_chunk in file_chunks:
                    start_chunk_time = datetime.now()
                    data = self.ingest_range(file_chunk[0], file_chunk[1])

                    blocks = data.table_contents["block"]
                    last_block_ts = sorted(blocks, key=lambda x: x["block_id"])[-1][
                        "timestamp"
                    ]
                    last_block_date = parse_timestamp(last_block_ts)

                    speed = (file_chunk[0] - file_chunk[1] + 1) / (
                        datetime.now() - start_chunk_time
                    ).total_seconds()
                    logger.info(
                        f"Written blocks: {file_chunk[0]:,} - {file_chunk[1]:,} "
                        f"""[{last_block_date.strftime(
                            GRAPHSENSE_DEFAULT_DATETIME_FORMAT
                        )}] """
                        f"({speed:.1f} blks/s)"
                    )

                    if check_shutdown_initialized():
                        break

        logger.info(
            f"Processed block range "
            f"{start_block:,} - {end_block:,} "
            f" ({last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)})"
        )
        self.ingest_blockindep()

        logger.info("Ingested blockindependent data. Finished")
