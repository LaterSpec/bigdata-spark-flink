#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Generic Parquet validation helper.")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--sample-size", type=int, default=5)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("ValidateParquetPath").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(args.input_path)
    print(f"VALIDATION_PATH={args.input_path}")
    print(f"VALIDATION_COUNT={df.count()}")
    print("VALIDATION_SCHEMA_BEGIN")
    df.printSchema()
    print("VALIDATION_SCHEMA_END")
    print("VALIDATION_SAMPLE_BEGIN")
    df.show(args.sample_size, truncate=80)
    print("VALIDATION_SAMPLE_END")
    spark.stop()


if __name__ == "__main__":
    main()

