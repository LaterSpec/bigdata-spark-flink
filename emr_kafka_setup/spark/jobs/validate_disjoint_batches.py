#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Validate two Spark batch parquet ranges.")
    parser.add_argument("--batch-a", required=True)
    parser.add_argument("--batch-b", required=True)
    parser.add_argument("--expected-count", type=int, default=0)
    return parser.parse_args()


def stats(df):
    rows = df.select(F.col("row_number").cast("long").alias("row_number")).cache()
    count = rows.count()
    row = rows.agg(
        F.min("row_number").alias("min_row"),
        F.max("row_number").alias("max_row"),
    ).first()
    rows.unpersist()
    return count, row["min_row"], row["max_row"]


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("ValidateDisjointKafkaBatches").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    batch_a = spark.read.parquet(args.batch_a)
    batch_b = spark.read.parquet(args.batch_b)
    a_count, a_min, a_max = stats(batch_a)
    b_count, b_min, b_max = stats(batch_b)
    overlap = (
        batch_a.select("event_id")
        .where(F.col("event_id").isNotNull())
        .join(batch_b.select("event_id").where(F.col("event_id").isNotNull()), "event_id")
        .count()
    )

    valid = overlap == 0 and a_max < b_min
    if args.expected_count:
        valid = valid and a_count == args.expected_count and b_count == args.expected_count

    print(f"BATCH_A_COUNT={a_count}")
    print(f"BATCH_A_RANGE={a_min}-{a_max}")
    print(f"BATCH_B_COUNT={b_count}")
    print(f"BATCH_B_RANGE={b_min}-{b_max}")
    print(f"EVENT_ID_OVERLAP={overlap}")
    print(f"DISJOINT_VALID={str(valid).lower()}")
    spark.stop()
    if not valid:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
