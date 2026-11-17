from pathlib import Path
import sys
import os
import csv
import time

import yaml
import logging
from dotenv import load_dotenv

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties, NonRetriableException, ZerobusException

PRODUCER_DIR = Path(__file__).resolve().parent
LAB_DIR = PRODUCER_DIR.parent
sys.path.insert(0, str(LAB_DIR / "proto"))
import result_pb2 as pb


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def load_config():
    load_dotenv(PRODUCER_DIR / ".env")
    with open(PRODUCER_DIR / "config.yml", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for name in ("ZEROBUS_CLIENT_ID", "ZEROBUS_CLIENT_SECRET"):
        if not os.getenv(name):
            raise RuntimeError(f"{name} is not in .env")

    return config


# bronze table column name: (CSV column name, type)
NOT_NULL_FIELDS = {
    "result_id":      ("resultId", int),
    "race_id":        ("raceId", int),
    "driver_id":      ("driverId", int),
    "constructor_id": ("constructorId", int),
    "grid":           ("grid", int),
    "position_text":  ("positionText", str),
    "position_order": ("positionOrder", int),
    "points":         ("points", float),
    "laps":           ("laps", int),
    "status_id":      ("statusId", int),
}
NULLABLE_FIELDS = {
    "number":            ("number", int),
    "position":          ("position", int),
    "time":              ("time", str),
    "milliseconds":      ("milliseconds", int),
    "fastest_lap":       ("fastestLap", int),
    "rank":              ("rank", int),
    "fastest_lap_time":  ("fastestLapTime", str),
    "fastest_lap_speed": ("fastestLapSpeed", float),
}
# results.csv marks missing values as \N
NULL_VALUES = ("", r"\N")


def to_record(row: dict, src_file: str):
    """Convert a CSV row to a protobuf message"""
    fields = {}

    for column, (csv_column, data_type) in NOT_NULL_FIELDS.items():
        raw = row[csv_column].strip()
        if raw in NULL_VALUES:
            log.warning("Skip resultId=%s: required column %s is empty", row["resultId"], csv_column)
            return None
        casted_value = data_type(raw)
        fields[column] = casted_value

    for column, (csv_column, data_type) in NULLABLE_FIELDS.items():
        raw = row[csv_column].strip()
        if raw not in NULL_VALUES:
            casted_value = data_type(raw)
            fields[column] = casted_value

    fields["_source_file"] = src_file
    fields["_produced_at"] = int(time.time() * 1000000)
    return pb.Result(**fields)


def main():
    config = load_config()
    zb_config = config["zerobus"]
    pd_config = config["producer"]

    csv_path = LAB_DIR / pd_config["csv_path"]
    batch_size = pd_config.get("batch_size", 500)

    sdk = ZerobusSdk(
        zb_config["server_endpoint"],
        zb_config["workspace_url"]
    )

    descriptor_bytes = pb.Result.DESCRIPTOR.file.serialized_pb
    table_properties = TableProperties(
        table_name=zb_config["table"],
        descriptor_proto=descriptor_bytes
    )

    stream = sdk.create_stream(
        os.environ["ZEROBUS_CLIENT_ID"],
        os.environ["ZEROBUS_CLIENT_SECRET"],
        table_properties
    )

    log.info("Stream opened to %s", zb_config["table"])

    skipped = 0
    sent = 0
    last_offset = None
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                record = to_record(row, csv_path.name)
                if record is None:
                    skipped += 1
                    continue

                last_offset = stream.ingest_record_offset(record)
                sent += 1

                if sent % batch_size == 0:
                    stream.wait_for_offset(last_offset)
                    log.info("ACKed %d records", sent)

        if last_offset is not None:
            stream.wait_for_offset(last_offset)
        stream.flush()
        log.info("Done: sent %d, skipped %d", sent, skipped)

    except NonRetriableException:
        log.exception("Non-retriable error: config, schema or permissions")
        raise

    except ZerobusException:
        log.exception("Zerobus error after SDK recovery attempts")
        raise

    finally:
        stream.close()


if __name__=="__main__":
    main()
