#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Apply Spark ML OffendES models to YouTube comments and optionally merge rules.

This script uses only pyspark.ml PipelineModels. No sklearn/joblib is used.
"""

from __future__ import print_function

import argparse

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_BUCKET = "s3://figuretibucket"
DEFAULT_INPUT = DEFAULT_BUCKET + "/data/raw/youtube/youtube_lake.csv"
DEFAULT_BINARY_MODEL = DEFAULT_BUCKET + "/output/batch/models/offendes_binary_sparkml"
DEFAULT_MULTICLASS_MODEL = DEFAULT_BUCKET + "/output/batch/models/offendes_multiclass_sparkml"
DEFAULT_PREDICTIONS = DEFAULT_BUCKET + "/output/batch/predictions/run_sparkml_offendes_full"
DEFAULT_RULES = DEFAULT_BUCKET + "/output/batch/predictions/run_rules_v1_full/"
DEFAULT_HYBRID = DEFAULT_BUCKET + "/output/batch/predictions/run_hybrid_sparkml_rules_full"


BINARY_LABELS = {
    0: "no_ofensivo",
    1: "ofensivo",
}

MULTICLASS_LABELS = {
    0: "ofensivo_directo",
    1: "odio_agresion_grupal",
    2: "neutral_no_ofensivo",
    3: "vulgaridad_contextual",
}

RULE_COLUMNS = [
    "has_terruqueo",
    "has_fraude",
    "has_electoral_institution",
    "has_political_mention",
    "has_polarization_signal",
    "has_discriminatory_language",
    "has_ethnic_racial_slur",
    "has_homophobic_slur",
    "has_general_insult",
    "is_spam_noise",
    "local_risk_score",
    "local_rule_tags",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Apply Spark ML OffendES models to YouTube.")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--binary-model", default=DEFAULT_BINARY_MODEL)
    parser.add_argument("--multiclass-model", default=DEFAULT_MULTICLASS_MODEL)
    parser.add_argument("--output-predictions", default=DEFAULT_PREDICTIONS)
    parser.add_argument("--rules-input", default=DEFAULT_RULES)
    parser.add_argument("--output-hybrid", default=DEFAULT_HYBRID)
    parser.add_argument("--coalesce", type=int, default=1)
    return parser.parse_args()


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    return fs.exists(hadoop_path)


def assert_absent(spark, path):
    if path_exists(spark, path):
        raise RuntimeError("Output path already exists; refusing to overwrite: " + path)


def clean_text_expr(col):
    text = F.lower(F.coalesce(col.cast("string"), F.lit("")))
    text = F.regexp_replace(text, r"https?://\S+|www\.\S+", " ")
    text = F.regexp_replace(text, r"[\r\n\t]+", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def prepare_youtube(df):
    return (
        df
        .withColumn("message_raw", F.coalesce(F.col("message").cast("string"), F.lit("")))
        .withColumn("text_clean", clean_text_expr(F.col("message_raw")))
        .withColumn("message_clean", F.col("text_clean"))
        .withColumn(
            "minute",
            F.when(
                F.col("video_offset_msec").cast("double").isNotNull(),
                F.floor(F.col("video_offset_msec").cast("double") / F.lit(60000)).cast("long"),
            ),
        )
    )


def prepare_rules(df):
    text_col = F.col("message_raw") if "message_raw" in df.columns else F.col("message")
    prepared = df.withColumn("message_raw", F.coalesce(text_col.cast("string"), F.lit("")))
    if "text_clean" not in prepared.columns:
        prepared = prepared.withColumn("text_clean", clean_text_expr(F.col("message_raw")))
    if "message_clean" not in prepared.columns:
        prepared = prepared.withColumn("message_clean", F.col("text_clean"))
    return prepared


def label_case(col, mapping):
    expr = None
    for key, value in mapping.items():
        condition = F.col(col).cast("int") == int(key)
        expr = F.when(condition, F.lit(value)) if expr is None else expr.when(condition, F.lit(value))
    return expr.otherwise(F.lit("unknown"))


def apply_models(df, binary_model, multiclass_model):
    def probability_at(probability, prediction):
        if probability is None or prediction is None:
            return None
        index = int(prediction)
        values = probability.toArray().tolist() if hasattr(probability, "toArray") else list(probability)
        if index < 0 or index >= len(values):
            return None
        return float(values[index])

    def probability_max(probability):
        if probability is None:
            return None
        values = probability.toArray().tolist() if hasattr(probability, "toArray") else list(probability)
        return float(max(values)) if values else None

    probability_at_udf = F.udf(probability_at, T.DoubleType())
    probability_max_udf = F.udf(probability_max, T.DoubleType())

    binary_df = binary_model.transform(df)
    for intermediate_col in ["tokens", "filtered_tokens", "raw_features", "features"]:
        if intermediate_col in binary_df.columns:
            binary_df = binary_df.drop(intermediate_col)
    both_df = multiclass_model.transform(binary_df)

    both_df = both_df.withColumn("pred_binary_sparkml", F.col("pred_binary_sparkml").cast("int"))
    both_df = both_df.withColumn("pred_multiclass_sparkml", F.col("pred_multiclass_sparkml").cast("int"))
    both_df = both_df.withColumn("pred_binary_sparkml_label", label_case("pred_binary_sparkml", BINARY_LABELS))
    both_df = both_df.withColumn("pred_multiclass_sparkml_label", label_case("pred_multiclass_sparkml", MULTICLASS_LABELS))

    both_df = both_df.withColumn(
        "confidence_binary_sparkml",
        probability_at_udf(F.col("pred_binary_sparkml_probability"), F.col("pred_binary_sparkml")),
    )
    both_df = both_df.withColumn(
        "confidence_multiclass_sparkml",
        probability_max_udf(F.col("pred_multiclass_sparkml_probability")),
    )
    return both_df


def select_existing(df, columns):
    return [col for col in columns if col in df.columns]


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("spark-apply-offendes-youtube").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    assert_absent(spark, args.output_predictions)
    assert_absent(spark, args.output_hybrid)

    binary_model = PipelineModel.load(args.binary_model)
    multiclass_model = PipelineModel.load(args.multiclass_model)

    youtube = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv(args.input)
    )
    youtube_prepared = prepare_youtube(youtube)
    predictions = apply_models(youtube_prepared, binary_model, multiclass_model)

    prediction_columns = [
        "source_file",
        "video_id",
        "timestamp_text",
        "timestamp_usec",
        "video_offset_msec",
        "minute",
        "author",
        "message_raw",
        "message_clean",
        "pred_binary_sparkml",
        "pred_binary_sparkml_label",
        "confidence_binary_sparkml",
        "pred_multiclass_sparkml",
        "pred_multiclass_sparkml_label",
        "confidence_multiclass_sparkml",
    ]
    predictions.select(*select_existing(predictions, prediction_columns)).coalesce(args.coalesce).write.mode("error").option("header", "true").csv(args.output_predictions)

    if args.rules_input:
        rules = (
            spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", "\"")
            .csv(args.rules_input)
        )
        rules_prepared = prepare_rules(rules)
        hybrid = apply_models(rules_prepared, binary_model, multiclass_model)
        hybrid_columns = prediction_columns + RULE_COLUMNS
        hybrid.select(*select_existing(hybrid, hybrid_columns)).coalesce(args.coalesce).write.mode("error").option("header", "true").csv(args.output_hybrid)
        hybrid_total = hybrid.count()
    else:
        hybrid_total = 0

    prediction_total = predictions.count()
    print("PREDICTIONS=" + args.output_predictions)
    print("PREDICTION_ROWS=" + str(prediction_total))
    print("HYBRID=" + args.output_hybrid)
    print("HYBRID_ROWS=" + str(hybrid_total))

    spark.stop()


if __name__ == "__main__":
    main()
