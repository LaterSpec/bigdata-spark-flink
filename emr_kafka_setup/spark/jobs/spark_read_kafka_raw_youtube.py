#!/usr/bin/env python3
import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, StructField, StructType


EXPECTED_FIELDS = [
    "event_id",
    "ingestion_ts",
    "source_s3_path",
    "row_number",
    "source_file",
    "video_id",
    "timestamp_text",
    "timestamp_usec",
    "video_offset_msec",
    "author",
    "author_channel_id",
    "message_raw",
    "message_clean",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read raw YouTube chat JSON events from Kafka and persist them as Parquet in S3."
    )
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="raw_youtube_chat")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--ending-offsets", default="latest")
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--report-path", default="")
    parser.add_argument("--coalesce", type=int, default=1)
    parser.add_argument("--min-row-number", type=int, default=0)
    parser.add_argument("--max-row-number", type=int, default=0)
    return parser.parse_args()


def build_schema():
    return StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("ingestion_ts", StringType(), True),
            StructField("source_s3_path", StringType(), True),
            StructField("row_number", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("video_id", StringType(), True),
            StructField("timestamp_text", StringType(), True),
            StructField("timestamp_usec", StringType(), True),
            StructField("video_offset_msec", StringType(), True),
            StructField("author", StringType(), True),
            StructField("author_channel_id", StringType(), True),
            StructField("message_raw", StringType(), True),
            StructField("message_clean", StringType(), True),
            StructField("raw", MapType(StringType(), StringType()), True),
        ]
    )


def markdown_escape(value):
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ")[:220]


def write_text(spark, path, content):
    jpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(jpath, True)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("KafkaRawYoutubeToS3")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("endingOffsets", args.ending_offsets)
        .load()
    )

    parsed = kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("json_value"),
    ).withColumn("event", F.from_json(F.col("json_value"), build_schema()))

    selected = parsed.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "json_value",
        *[F.col(f"event.{field}").alias(field) for field in EXPECTED_FIELDS],
        F.col("event.raw").alias("raw"),
    )

    with_source_file = selected.withColumn(
        "source_file",
        F.coalesce(
            F.col("source_file"),
            F.col("raw").getItem("source_file"),
            F.col("raw").getItem("\ufeffsource_file"),
        ),
    )

    output_df = with_source_file.select(
        *EXPECTED_FIELDS,
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "json_value",
        "raw",
    )
    row_number = F.col("row_number").cast("long")
    if args.min_row_number > 0:
        output_df = output_df.filter(row_number >= F.lit(args.min_row_number))
    if args.max_row_number > 0:
        output_df = output_df.filter(row_number <= F.lit(args.max_row_number))

    writer_df = output_df.coalesce(args.coalesce) if args.coalesce > 0 else output_df
    writer_df.write.mode("overwrite").parquet(args.output_path)

    total_records = output_df.count()
    offset_rows = (
        output_df.groupBy("kafka_partition")
        .agg(
            F.min("kafka_offset").alias("min_offset"),
            F.max("kafka_offset").alias("max_offset"),
            F.count("*").alias("records"),
        )
        .orderBy("kafka_partition")
        .collect()
    )
    sample_rows = (
        output_df.select(
            "event_id",
            "row_number",
            "video_id",
            "timestamp_text",
            "author",
            "message_raw",
            "kafka_partition",
            "kafka_offset",
        )
        .orderBy("kafka_partition", "kafka_offset")
        .limit(5)
        .collect()
    )

    columns = output_df.columns
    report_path = args.report_path
    if report_path:
        offset_lines = "\n".join(
            f"| {row['kafka_partition']} | {row['min_offset']} | {row['max_offset']} | {row['records']} |"
            for row in offset_rows
        )
        sample_lines = "\n".join(
            "| {event_id} | {row_number} | {video_id} | {timestamp_text} | {author} | {message_raw} | {partition} | {offset} |".format(
                event_id=markdown_escape(row["event_id"]),
                row_number=markdown_escape(row["row_number"]),
                video_id=markdown_escape(row["video_id"]),
                timestamp_text=markdown_escape(row["timestamp_text"]),
                author=markdown_escape(row["author"]),
                message_raw=markdown_escape(row["message_raw"]),
                partition=markdown_escape(row["kafka_partition"]),
                offset=markdown_escape(row["kafka_offset"]),
            )
            for row in sample_rows
        )
        command = (
            "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 "
            "/home/hadoop/bigdata-kafka/spark/jobs/spark_read_kafka_raw_youtube.py "
            f"--bootstrap-server {args.bootstrap_server} "
            f"--topic {args.topic} "
            f"--starting-offsets {args.starting_offsets} "
            f"--ending-offsets {args.ending_offsets} "
            f"--output-path {args.output_path} "
            f"--report-path {report_path}"
        )
        report = f"""# Spark Kafka consumer report

## Resumen

- Fecha de ejecucion: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}
- Bootstrap server: `{args.bootstrap_server}`
- Topic: `{args.topic}`
- Starting offsets: `{args.starting_offsets}`
- Ending offsets: `{args.ending_offsets}`
- Output parquet: `{args.output_path}`
- Total de registros leidos: `{total_records}`

## Offsets por particion

| kafka_partition | min_offset | max_offset | records |
|---:|---:|---:|---:|
{offset_lines}

## Columnas generadas

```text
{chr(10).join(columns)}
```

## Muestra de 5 registros

| event_id | row_number | video_id | timestamp_text | author | message_raw | partition | offset |
|---|---:|---|---|---|---|---:|---:|
{sample_lines}

## Comando ejecutado

```bash
{command}
```

## Limitaciones

- Esta prueba usa Kafka self-managed en el master EMR por restricciones academicas de AWS Academy.
- Se leyeron los mensajes ya presentes en `raw_youtube_chat`; no se ejecuto la carga completa de 160,464 comentarios.
- El job es una base de ingesta batch desde Kafka. Las reglas peruanas, OffendES Spark ML, scoring hibrido y agregados historicos se conectaran en fases posteriores.
- El output se escribe en modo `overwrite` para mantener reproducible esta prueba puntual.
"""
        write_text(spark, report_path.rstrip("/") + "/report.md", report)

    print(f"TOTAL_RECORDS={total_records}")
    print(f"OUTPUT_PATH={args.output_path}")
    print(f"COLUMNS={','.join(columns)}")
    for row in offset_rows:
        print(
            "OFFSET_SUMMARY partition={partition} min={min_offset} max={max_offset} records={records}".format(
                partition=row["kafka_partition"],
                min_offset=row["min_offset"],
                max_offset=row["max_offset"],
                records=row["records"],
            )
        )

    spark.stop()


if __name__ == "__main__":
    main()
