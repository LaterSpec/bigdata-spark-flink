#!/usr/bin/env python3
import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


LOCAL_FLAGS = [
    "has_terruqueo", "has_fraude", "has_electoral_institution",
    "has_political_mention", "has_polarization_signal",
    "has_discriminatory_language", "has_ethnic_racial_slur",
    "has_homophobic_slur", "has_general_insult", "is_spam_noise",
]
STRONG_FLAGS = [
    "has_terruqueo", "has_fraude", "has_polarization_signal",
    "has_discriminatory_language", "has_homophobic_slur", "has_general_insult",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Hybrid scoring for Kafka-origin Spark outputs.")
    parser.add_argument("--rules-input", required=True)
    parser.add_argument("--ml-input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--aggregates-output", required=True)
    parser.add_argument("--report-output", required=True)
    parser.add_argument("--coalesce", type=int, default=1)
    return parser.parse_args()


def write_text(spark, path, content):
    jpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(jpath, True)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def bool_col(name):
    return F.coalesce(F.col(name).cast("boolean"), F.lit(False))


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("hybrid-scoring-from-kafka").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    rules = spark.read.parquet(args.rules_input)
    ml = spark.read.parquet(args.ml_input)
    total_rules = rules.count()
    total_ml = ml.count()

    kafka_join_cols = ["kafka_topic", "kafka_partition", "kafka_offset"]
    if all(c in rules.columns for c in kafka_join_cols) and all(c in ml.columns for c in kafka_join_cols):
        join_cols = kafka_join_cols
    else:
        join_cols = ["event_id"]

    duplicated_identity_cols = [
        "message_raw", "message_clean", "source_file", "video_id", "timestamp_text",
        "timestamp_usec", "video_offset_msec", "author", "author_channel_id",
        "row_number", "source_s3_path", "kafka_topic", "kafka_partition",
        "kafka_offset", "kafka_timestamp", "minute", "event_id",
    ]
    ml_cols_to_drop = [c for c in duplicated_identity_cols if c in ml.columns and c not in join_cols]
    ml_reduced = ml.drop(*ml_cols_to_drop)
    joined = rules.join(ml_reduced, on=join_cols, how="inner")

    for flag in LOCAL_FLAGS:
        joined = joined.withColumn(flag, bool_col(flag))
    joined = joined.withColumn("local_risk_score", F.coalesce(F.col("local_risk_score").cast("int"), F.lit(0)))
    joined = joined.withColumn("confidence_binary_sparkml", F.coalesce(F.col("confidence_binary_sparkml").cast("double"), F.lit(0.0)))
    joined = joined.withColumn("pred_binary_sparkml", F.coalesce(F.col("pred_binary_sparkml").cast("int"), F.lit(0)))

    any_rule = None
    for flag in LOCAL_FLAGS:
        any_rule = F.col(flag) if any_rule is None else (any_rule | F.col(flag))
    strong_rule = None
    for flag in STRONG_FLAGS:
        strong_rule = F.col(flag) if strong_rule is None else (strong_rule | F.col(flag))

    scored = joined.withColumn("has_any_local_rule", any_rule)
    scored = scored.withColumn("has_strong_local_rule", strong_rule)
    scored = scored.withColumn("ml_offensive_high_confidence", (F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") >= 0.70))
    if "confidence_binary_bucket" not in scored.columns:
        scored = scored.withColumn(
            "confidence_binary_bucket",
            F.when((F.col("confidence_binary_sparkml") >= 0.5) & (F.col("confidence_binary_sparkml") < 0.6), "0.50-0.60")
            .when((F.col("confidence_binary_sparkml") >= 0.6) & (F.col("confidence_binary_sparkml") < 0.7), "0.60-0.70")
            .when((F.col("confidence_binary_sparkml") >= 0.7) & (F.col("confidence_binary_sparkml") < 0.8), "0.70-0.80")
            .when((F.col("confidence_binary_sparkml") >= 0.8) & (F.col("confidence_binary_sparkml") < 0.9), "0.80-0.90")
            .when((F.col("confidence_binary_sparkml") >= 0.9) & (F.col("confidence_binary_sparkml") <= 1.0), "0.90-1.00")
            .otherwise("out_of_range"),
        )

    scored = scored.withColumn(
        "hybrid_risk_level",
        F.when((F.col("ml_offensive_high_confidence") & F.col("has_strong_local_rule")) | (F.col("local_risk_score") >= 5), "alto")
        .when(((F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") >= 0.60)) | (F.col("local_risk_score") >= 2), "medio")
        .otherwise("bajo"),
    )
    scored = scored.withColumn(
        "hybrid_risk_reason",
        F.when(F.col("ml_offensive_high_confidence") & F.col("has_strong_local_rule"), "ML ofensivo alta confianza + reglas locales fuertes")
        .when(F.col("local_risk_score") >= 5, "Reglas locales muy fuertes")
        .when((F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") >= 0.60) & F.col("has_any_local_rule"), "ML ofensivo + reglas locales")
        .when((F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") >= 0.60), "ML ofensivo sin evidencia local fuerte; revisar")
        .when(F.col("local_risk_score") >= 2, "Reglas locales activas")
        .otherwise("Bajo riesgo hibrido"),
    )
    scored = scored.cache()
    total_out = scored.count()
    scored.coalesce(args.coalesce).write.mode("overwrite").parquet(args.output)

    aggregates = (
        scored.groupBy("source_file", "video_id", "minute")
        .agg(
            F.count("*").alias("comentarios"),
            F.sum(F.when(F.col("pred_binary_sparkml") == 1, 1).otherwise(0)).alias("ml_ofensivos"),
            F.sum(F.when(F.col("has_terruqueo"), 1).otherwise(0)).alias("terruqueo"),
            F.sum(F.when(F.col("has_fraude"), 1).otherwise(0)).alias("fraude"),
            F.sum(F.when(F.col("has_polarization_signal"), 1).otherwise(0)).alias("polarizacion"),
            F.avg("local_risk_score").alias("avg_local_risk_score"),
        )
        .orderBy("source_file", "video_id", "minute")
    )
    aggregates.coalesce(args.coalesce).write.mode("overwrite").parquet(args.aggregates_output)

    risk_rows = scored.groupBy("hybrid_risk_level").count().orderBy("hybrid_risk_level").collect()
    binary_rows = scored.groupBy("pred_binary_sparkml_label").count().orderBy("pred_binary_sparkml_label").collect()
    high_conf = scored.filter(F.col("ml_offensive_high_confidence")).count()
    lines = [
        "# Job 5 - Scoring Hibrido desde Kafka",
        "",
        f"- Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"- Rules input: `{args.rules_input}`",
        f"- ML input: `{args.ml_input}`",
        f"- Output: `{args.output}`",
        f"- Aggregates: `{args.aggregates_output}`",
        f"- Rules rows: `{total_rules}`",
        f"- ML rows: `{total_ml}`",
        f"- Output rows: `{total_out}`",
        f"- ML offensive high confidence: `{high_conf}`",
        "",
        "## Distribucion hybrid_risk_level",
        "",
        "| level | count | percentage |",
        "|---|---:|---:|",
    ]
    for row in risk_rows:
        pct = round(row["count"] / total_out * 100.0, 4) if total_out else 0.0
        lines.append(f"| {row['hybrid_risk_level']} | {row['count']} | {pct}% |")
    lines.extend(["", "## Distribucion ML binaria", "", "| label | count | percentage |", "|---|---:|---:|"])
    for row in binary_rows:
        pct = round(row["count"] / total_out * 100.0, 4) if total_out else 0.0
        lines.append(f"| {row['pred_binary_sparkml_label']} | {row['count']} | {pct}% |")
    lines.extend([
        "",
        "## Interpretacion",
        "",
        "El resultado defendible es `hybrid_risk_level`, no la prediccion ML cruda. OffendES aporta una senal general de ofensividad en espanol, mientras que las reglas locales aportan contexto peruano como terruqueo, fraude, instituciones electorales y polarizacion.",
    ])
    write_text(spark, args.report_output.rstrip("/") + "/summary.md", "\n".join(lines) + "\n")

    print(f"RULES_ROWS={total_rules}")
    print(f"ML_ROWS={total_ml}")
    print(f"OUTPUT_ROWS={total_out}")
    print(f"OUTPUT={args.output}")
    print(f"AGGREGATES={args.aggregates_output}")
    print(f"REPORT={args.report_output.rstrip('/')}/summary.md")
    scored.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
