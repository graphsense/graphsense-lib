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
    """Converts a log file as exported by the ingest component into a
    dsbulk imporable format.

    Ingest can be invoked as follows:
        dsbulk load -c csv -h ip -k eth_raw -t log -url PATH_TO_FILES \
         --connector.csv.fileNamePattern '/logs_abc.csv.gz' \
         --connector.csv.compression gzip --connector.csv.recursive true \
         --log.verbosity 2 --connector.csv.delimiter "|" \
         --connector.csv.maxCharsPerColumn 100000 \
         --dsbulk.connector.csv.nullValue "" \
         --dsbulk.connector.csv.ignoreLeadingWhitespaces true \
         --dsbulk.connector.csv.ignoreTrailingWhitespaces true


    Args:
        filename (str): Description
        outfile_suffix (str): Description

    Raises:
        e: Description
    """

    # set max fieldsize to 1MB, some logs have big data fields.
    # Default 16kb fail on certain logs.
    csv.field_size_limit(8388608)
    outfile = f"{filename}.{outfile_suffix}"

    if os.path.exists(outfile):
        logger.warning(f"Output file {outfile} already exists. I am done here.")
        sys.exit(199)

    if not filename.endswith("csv.gz"):
        logger.warning(f"Output file {outfile} name does not end with csv.gz.")
        sys.exit(201)
    try:
        with gzip.open(filename, "rt") as f:
            r = csv.DictReader(f, delimiter=",", quotechar='"')
            with gzip.open(outfile, "wt") as wf:
                w = csv.DictWriter(
                    wf,
                    fieldnames=ingestable_logs_fields,
                    delimiter="|",
                    quoting=csv.QUOTE_NONE,
                    quotechar="",
                )
                w.writeheader()
                for row in r:
                    if "transaction_hash" in row:
                        row.pop("transaction_hash")

                    tpcs_str = row["topics"].strip()

                    tpcs = tpcs_str.split("|") if len(tpcs_str) > 0 else []

                    if "topic0" not in row:
                        row["topic0"] = tpcs[0] if len(tpcs) > 0 else None

                    qt = ",".join([f'"{t}"' for t in tpcs])
                    row["topics"] = f"[{qt}]"

                    w.writerow(row)

    except Exception as e:
        if os.path.exists(outfile):
            os.remove(outfile)
        logger.error(f"Caught exception removed outfile {outfile}.")
        raise e
