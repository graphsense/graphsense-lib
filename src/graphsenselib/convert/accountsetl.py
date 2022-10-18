import csv
import gzip
import logging
import os.path
import sys

logger = logging.getLogger(__name__)

ingestable_logs_fields = [
    "block_id_group",
    "block_id",
    "block_hash",
    "address",
    "data",
    "topics",
    "topic0",
    "tx_hash",
    "log_index",
    "transaction_index",
]


def convert_etl_to_ingestable_logs(filename: str, outfile_suffix: str):
    outfile = f"{filename}.{outfile_suffix}"

    if os.path.exists(outfile):
        logger.warning(f"Output file {outfile} already exists. I am done here.")
        sys.exit(199)

    if not filename.endswith("csv.gz"):
        logger.warning(f"Output file {outfile} name does not end with csv.gz.")
        sys.exit(201)
    try:
        with gzip.open(filename, "rt") as f:
            with gzip.open(outfile, "wt") as wf:
                r = csv.DictReader(f, delimiter=",", quotechar='"')
                w = csv.DictWriter(
                    wf, fieldnames=ingestable_logs_fields, delimiter="|", quotechar='"'
                )
                w.writeheader()
                for row in r:
                    if "transaction_hash" in row:
                        row.pop("transaction_hash")

                    tpcs = row["topics"].split("|")

                    if "topic0" not in row:
                        row["topic0"] = tpcs[0] if len(tpcs) > 0 else None
                        row["topics"] = f"{','.join(tpcs)}"

                    logger.debug(",".join(row.values()))

                    w.writerow(row)

    except Exception as e:
        if os.path.exists(outfile):
            os.remove(outfile)
        raise e
