#!/usr/bin/env python3
import argparse
from datetime import datetime, timezone

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


BINARY_LABELS = {0: "no_ofensivo", 1: "ofensivo"}
MULTICLASS_LABELS = {
    0: "ofensivo_directo",
    1: "odio_agresion_grupal",
    2: "neutral_no_ofensivo",
    3: "vulgaridad_contextual",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Apply existing OffendES Spark ML models to Kafka-origin parquet.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--binary-model", required=True)
    parser.add_argument("--multiclass-model", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report-output", required=True)
    parser.add_argument("--coalesce", type=int, default=1)
    return parser.parse_args()


def clean_text_expr(col):
    text = F.lower(F.coalesce(col.cast("string"), F.lit("")))
    text = F.regexp_replace(text, r"https?://\S+|www\.\S+", " ")
    text = F.regexp_replace(text, r"[\r\n\t]+", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def label_case(col, mapping):
    expr = None
    for key, value in mapping.items():
        condition = F.col(col).cast("int") == int(key)
        expr = F.when(condition, F.lit(value)) if expr is None else expr.when(condition, F.lit(value))
    return expr.otherwise(F.lit("unknown"))


def confidence_bucket(col):
    return (
        F.when((F.col(col) >= 0.5) & (F.col(col) < 0.6), "0.50-0.60")
        .when((F.col(col) >= 0.6) & (F.col(col) < 0.7), "0.60-0.70")
        .when((F.col(col) >= 0.7) & (F.col(col) < 0.8), "0.70-0.80")
        .when((F.col(col) >= 0.8) & (F.col(col) < 0.9), "0.80-0.90")
        .when((F.col(col) >= 0.9) & (F.col(col) <= 1.0), "0.90-1.00")
        .otherwise("out_of_range")
    )


def write_text(spark, path, content):
    jpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(jpath, True)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def apply_models(df, binary_model, multiclass_model):
    def prob_at(probability, prediction):
        if probability is None or prediction is None:
            return None
        values = probability.toArray().tolist() if hasattr(probability, "toArray") else list(probability)
        idx = int(prediction)
        return float(values[idx]) if 0 <= idx < len(values) else None

    def prob_max(probability):
        if probability is None:
            return None
        values = probability.toArray().tolist() if hasattr(probability, "toArray") else list(probability)
        return float(max(values)) if values else None

    prob_at_udf = F.udf(prob_at, T.DoubleType())
    prob_max_udf = F.udf(prob_max, T.DoubleType())

    base = df.withColumn("message_raw", F.coalesce(F.col("message_raw"), F.lit("")))
    base = base.withColumn("message_clean", F.coalesce(F.col("message_clean"), F.col("message_raw"), F.lit("")))
    base = base.withColumn("text_clean", clean_text_expr(F.col("message_clean")))
    binary_df = binary_model.transform(base)
    for col in ["tokens", "filtered_tokens", "raw_features", "features"]:
        if col in binary_df.columns:
            binary_df = binary_df.drop(col)
    both = multiclass_model.transform(binary_df)
    both = both.withColumn("pred_binary_sparkml", F.col("pred_binary_sparkml").cast("int"))
    both = both.withColumn("pred_multiclass_sparkml", F.col("pred_multiclass_sparkml").cast("int"))
    both = both.withColumn("pred_binary_sparkml_label", label_case("pred_binary_sparkml", BINARY_LABELS))
    both = both.withColumn("pred_multiclass_sparkml_label", label_case("pred_multiclass_sparkml", MULTICLASS_LABELS))
    both = both.withColumn("confidence_binary_sparkml", prob_at_udf(F.col("pred_binary_sparkml_probability"), F.col("pred_binary_sparkml")))
    both = both.withColumn("confidence_multiclass_sparkml", prob_max_udf(F.col("pred_multiclass_sparkml_probability")))
    both = both.withColumn("confidence_binary_bucket", confidence_bucket("confidence_binary_sparkml"))
    return both


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("offendes-from-kafka-parquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    binary_model = PipelineModel.load(args.binary_model)
    multiclass_model = PipelineModel.load(args.multiclass_model)
    source = spark.read.parquet(args.input)
    total_in = source.count()
    predictions = apply_models(source, binary_model, multiclass_model).cache()
    total_out = predictions.count()

    cols = [
        "event_id", "row_number", "source_s3_path", "kafka_topic", "kafka_partition",
        "kafka_offset", "kafka_timestamp", "source_file", "video_id", "timestamp_text",
        "timestamp_usec", "video_offset_msec", "author", "author_channel_id",
        "message_raw", "message_clean", "text_clean", "pred_binary_sparkml",
        "pred_binary_sparkml_label", "confidence_binary_sparkml", "confidence_binary_bucket",
        "pred_multiclass_sparkml", "pred_multiclass_sparkml_label",
        "confidence_multiclass_sparkml",
    ]
    predictions.select(*[c for c in cols if c in predictions.columns]).coalesce(args.coalesce).write.mode("overwrite").parquet(args.output)

    binary_rows = predictions.groupBy("pred_binary_sparkml_label").count().orderBy("pred_binary_sparkml_label").collect()
    multi_rows = predictions.groupBy("pred_multiclass_sparkml_label").count().orderBy("pred_multiclass_sparkml_label").collect()
    bucket_rows = predictions.groupBy("confidence_binary_bucket").count().orderBy("confidence_binary_bucket").collect()
    lines = [
        "# Job 4 - Inferencia OffendES sobre datos Kafka",
        "",
        f"- Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"- Input: `{args.input}`",
        f"- Output: `{args.output}`",
        f"- Binary model: `{args.binary_model}`",
        f"- Multiclass model: `{args.multiclass_model}`",
        f"- Input rows: `{total_in}`",
        f"- Output rows: `{total_out}`",
        "",
        "## Distribucion binaria",
        "",
        "| label | count | percentage |",
        "|---|---:|---:|",
    ]
    for row in binary_rows:
        pct = round(row["count"] / total_out * 100.0, 4) if total_out else 0.0
        lines.append(f"| {row['pred_binary_sparkml_label']} | {row['count']} | {pct}% |")
    lines.extend(["", "## Distribucion multiclase", "", "| label | count | percentage |", "|---|---:|---:|"])
    for row in multi_rows:
        pct = round(row["count"] / total_out * 100.0, 4) if total_out else 0.0
        lines.append(f"| {row['pred_multiclass_sparkml_label']} | {row['count']} | {pct}% |")
    lines.extend(["", "## Buckets confidence_binary_sparkml", "", "| bucket | count |", "|---|---:|"])
    for row in bucket_rows:
        lines.append(f"| {row['confidence_binary_bucket']} | {row['count']} |")
    write_text(spark, args.report_output.rstrip("/") + "/summary.md", "\n".join(lines) + "\n")

    print(f"INPUT_ROWS={total_in}")
    print(f"OUTPUT_ROWS={total_out}")
    print(f"OUTPUT={args.output}")
    print(f"REPORT={args.report_output.rstrip('/')}/summary.md")
    predictions.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

