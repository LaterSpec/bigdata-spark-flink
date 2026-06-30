#!/usr/bin/env python3
import argparse
import csv
import hashlib
import io
import json
import logging
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from kafka import KafkaProducer


PREFERRED_FIELDS = [
    "source_file",
    "video_id",
    "timestamp_text",
    "timestamp_usec",
    "video_offset_msec",
    "author",
    "author_channel_id",
    "message_raw",
    "message_clean",
    "message",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish YouTube Live Chat CSV rows from S3 to Kafka as JSON."
    )
    parser.add_argument("--s3-path", required=True)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="raw_youtube_chat")
    parser.add_argument("--start-row", type=int, default=1, help="First CSV data row to publish (1-based)")
    parser.add_argument("--limit", type=int, default=0, help="0 means no limit")
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--log-every", type=int, default=25)
    return parser.parse_args()


def parse_s3_uri(s3_uri):
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError("Expected S3 URI like s3://bucket/key.csv")
    return parsed.netloc, parsed.path.lstrip("/")


def stable_event_id(source_s3_path, row_number, row):
    key_parts = [
        row.get("video_id", ""),
        row.get("timestamp_usec", ""),
        row.get("author_channel_id", ""),
        row.get("author", ""),
        str(row_number),
    ]
    raw_key = "|".join(key_parts)
    digest = hashlib.sha256((source_s3_path + "|" + raw_key).encode("utf-8")).hexdigest()
    return digest[:32]


def kafka_key(row):
    key_parts = [
        row.get("video_id", ""),
        row.get("timestamp_usec", ""),
        row.get("author_channel_id", ""),
        row.get("author", ""),
    ]
    key = "|".join(part for part in key_parts if part)
    return key.encode("utf-8") if key else None


def build_event(row, row_number, source_s3_path, missing_fields_logged):
    missing_fields = [field for field in PREFERRED_FIELDS if field not in row]
    if missing_fields and not missing_fields_logged:
        logging.warning("Missing expected columns: %s", ", ".join(missing_fields))
        missing_fields_logged.add("logged")

    message_raw = row.get("message_raw") or row.get("message") or ""
    event = {
        "event_id": stable_event_id(source_s3_path, row_number, row),
        "ingestion_ts": datetime.now(timezone.utc).isoformat(),
        "source_s3_path": source_s3_path,
        "row_number": row_number,
        "video_id": row.get("video_id"),
        "timestamp_text": row.get("timestamp_text"),
        "timestamp_usec": row.get("timestamp_usec"),
        "video_offset_msec": row.get("video_offset_msec"),
        "author": row.get("author"),
        "author_channel_id": row.get("author_channel_id"),
        "message_raw": message_raw,
        "message_clean": row.get("message_clean") or message_raw,
        "raw": {key: value for key, value in row.items() if key},
    }
    return event


def clean_row(row):
    return {
        (key or "").lstrip("\ufeff").strip(): value
        for key, value in row.items()
    }


def main():
    args = parse_args()
    if args.start_row < 1:
        raise ValueError("--start-row must be >= 1")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    bucket, key = parse_s3_uri(args.s3_path)
    logging.info("Reading CSV from %s", args.s3_path)
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        linger_ms=10,
        retries=5,
    )

    sent = 0
    missing_fields_logged = set()
    delay_seconds = max(args.delay_ms, 0) / 1000.0

    text_stream = io.TextIOWrapper(body, encoding="utf-8-sig", newline="")
    reader = csv.DictReader(text_stream)

    try:
        for row_number, raw_row in enumerate(reader, start=1):
            if row_number < args.start_row:
                continue
            if args.limit and sent >= args.limit:
                break

            row = clean_row(raw_row)
            event = build_event(row, row_number, args.s3_path, missing_fields_logged)
            producer.send(args.topic, key=kafka_key(row), value=event)
            sent += 1

            if sent == 1 or sent % args.log_every == 0:
                logging.info("Published %s messages to topic %s", sent, args.topic)

            if delay_seconds:
                time.sleep(delay_seconds)

        producer.flush()
        logging.info("Finished publishing %s messages to %s", sent, args.topic)
    finally:
        producer.close()
        body.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
