#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Train OffendES Spark ML baselines.

Outputs Spark-native PipelineModels for binary and multiclass classification.
No sklearn/joblib is used.
"""

from __future__ import print_function

import argparse
import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_BUCKET = "s3://figuretibucket"
DEFAULT_TRAIN = DEFAULT_BUCKET + "/dataset/offendES/train.parquet"
DEFAULT_VALIDATION = DEFAULT_BUCKET + "/dataset/offendES/validation.parquet"
DEFAULT_TEST = DEFAULT_BUCKET + "/dataset/offendES/test.parquet"
DEFAULT_BINARY_MODEL = DEFAULT_BUCKET + "/output/batch/models/offendes_binary_sparkml"
DEFAULT_MULTICLASS_MODEL = DEFAULT_BUCKET + "/output/batch/models/offendes_multiclass_sparkml"
DEFAULT_REPORT = DEFAULT_BUCKET + "/output/batch/reports/spark_ml_training"


LABEL_NAMES = {
    0: "ofensivo_directo",
    1: "odio_agresion_grupal",
    2: "neutral_no_ofensivo",
    3: "vulgaridad_contextual",
}

BINARY_NAMES = {
    0: "no_ofensivo",
    1: "ofensivo",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Train Spark ML OffendES baselines.")
    parser.add_argument("--train", default=DEFAULT_TRAIN)
    parser.add_argument("--validation", default=DEFAULT_VALIDATION)
    parser.add_argument("--test", default=DEFAULT_TEST)
    parser.add_argument("--binary-model-output", default=DEFAULT_BINARY_MODEL)
    parser.add_argument("--multiclass-model-output", default=DEFAULT_MULTICLASS_MODEL)
    parser.add_argument("--report-output", default=DEFAULT_REPORT)
    parser.add_argument("--num-features", type=int, default=1 << 18)
    parser.add_argument("--max-iter", type=int, default=60)
    parser.add_argument("--reg-param", type=float, default=0.02)
    return parser.parse_args()


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    return fs.exists(hadoop_path)


def assert_absent(spark, path):
    if path_exists(spark, path):
        raise RuntimeError("Output path already exists; refusing to overwrite: " + path)


def write_text_file(spark, path, content):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    if fs.exists(hadoop_path):
        raise RuntimeError("Report path already exists; refusing to overwrite: " + path)
    stream = fs.create(hadoop_path, False)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def clean_text_expr(col):
    text = F.lower(F.coalesce(col.cast("string"), F.lit("")))
    text = F.regexp_replace(text, r"https?://\S+|www\.\S+", " ")
    text = F.regexp_replace(text, r"[\r\n\t]+", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def prepare_base(df):
    return (
        df
        .withColumn("label_original", F.col("label").cast("double"))
        .withColumn(
            "label_binary",
            F.when(F.col("label").isin([0, 1]), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
        .withColumn("text_raw", F.coalesce(F.col("comment").cast("string"), F.lit("")))
        .withColumn("text_clean", clean_text_expr(F.col("comment")))
        .filter(F.length(F.col("text_clean")) > 0)
    )


def add_class_weights(df, label_col, weight_col):
    total = df.count()
    counts = {row[label_col]: row["count"] for row in df.groupBy(label_col).count().collect()}
    num_classes = len(counts)
    mapping = []
    for label_value, count_value in counts.items():
        weight = float(total) / float(num_classes * count_value) if count_value else 1.0
        mapping.extend([F.lit(float(label_value)), F.lit(float(weight))])
    return df.withColumn(weight_col, F.create_map(mapping)[F.col(label_col)])


def spanish_stopwords():
    try:
        return StopWordsRemover.loadDefaultStopWords("spanish")
    except Exception:
        return [
            "de", "la", "que", "el", "en", "y", "a", "los", "del", "se",
            "las", "por", "un", "para", "con", "no", "una", "su", "al",
            "lo", "es", "como", "mas", "pero", "sus", "le", "ya", "o",
        ]


def build_pipeline(label_col, weight_col, args, prediction_col):
    tokenizer = RegexTokenizer(
        inputCol="text_clean",
        outputCol="tokens",
        pattern="\\W+",
        gaps=True,
        minTokenLength=2,
        toLowercase=True,
    )
    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filtered_tokens",
        stopWords=spanish_stopwords(),
    )
    hashing = HashingTF(
        inputCol="filtered_tokens",
        outputCol="raw_features",
        numFeatures=args.num_features,
    )
    idf = IDF(inputCol="raw_features", outputCol="features")
    lr = LogisticRegression(
        featuresCol="features",
        labelCol=label_col,
        weightCol=weight_col,
        predictionCol=prediction_col,
        probabilityCol=prediction_col + "_probability",
        rawPredictionCol=prediction_col + "_raw",
        maxIter=args.max_iter,
        regParam=args.reg_param,
        family="auto",
    )
    return Pipeline(stages=[tokenizer, remover, hashing, idf, lr])


def evaluator(metric, label_col, prediction_col):
    return MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol=prediction_col,
        metricName=metric,
    )


def confusion_and_macro(predictions, label_col, prediction_col, labels):
    matrix_rows = (
        predictions
        .groupBy(label_col, prediction_col)
        .count()
        .orderBy(label_col, prediction_col)
        .collect()
    )
    counts = {(int(r[label_col]), int(r[prediction_col])): int(r["count"]) for r in matrix_rows}
    per_class = []
    for label_value in labels:
        tp = counts.get((label_value, label_value), 0)
        predicted_total = sum(counts.get((actual, label_value), 0) for actual in labels)
        actual_total = sum(counts.get((label_value, pred), 0) for pred in labels)
        precision = float(tp) / float(predicted_total) if predicted_total else 0.0
        recall = float(tp) / float(actual_total) if actual_total else 0.0
        f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        per_class.append({
            "label": label_value,
            "actual_total": actual_total,
            "predicted_total": predicted_total,
            "tp": tp,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        })
    macro_precision = sum(row["precision"] for row in per_class) / float(len(labels))
    macro_recall = sum(row["recall"] for row in per_class) / float(len(labels))
    macro_f1 = sum(row["f1"] for row in per_class) / float(len(labels))
    return counts, per_class, {
        "macro_precision": macro_precision,
        "macro_recall": macro_recall,
        "macro_f1": macro_f1,
    }


def evaluate_model(predictions, label_col, prediction_col, labels):
    metrics = {
        "accuracy": evaluator("accuracy", label_col, prediction_col).evaluate(predictions),
        "weighted_precision": evaluator("weightedPrecision", label_col, prediction_col).evaluate(predictions),
        "weighted_recall": evaluator("weightedRecall", label_col, prediction_col).evaluate(predictions),
        "weighted_f1": evaluator("f1", label_col, prediction_col).evaluate(predictions),
    }
    confusion, per_class, macro = confusion_and_macro(predictions, label_col, prediction_col, labels)
    metrics.update(macro)
    return metrics, confusion, per_class


def distribution(df, label_col):
    rows = df.groupBy(label_col).count().orderBy(label_col).collect()
    return {str(int(row[label_col])): int(row["count"]) for row in rows}


def markdown_table_metrics(title, metrics):
    return [
        "### " + title,
        "",
        "| metric | value |",
        "|---|---:|",
        "| accuracy | {:.6f} |".format(metrics["accuracy"]),
        "| weighted_precision | {:.6f} |".format(metrics["weighted_precision"]),
        "| weighted_recall | {:.6f} |".format(metrics["weighted_recall"]),
        "| weighted_f1 | {:.6f} |".format(metrics["weighted_f1"]),
        "| macro_precision | {:.6f} |".format(metrics["macro_precision"]),
        "| macro_recall | {:.6f} |".format(metrics["macro_recall"]),
        "| macro_f1 | {:.6f} |".format(metrics["macro_f1"]),
        "",
    ]


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("spark-train-offendes").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for output_path in [args.binary_model_output, args.multiclass_model_output, args.report_output]:
        assert_absent(spark, output_path)

    train = prepare_base(spark.read.parquet(args.train)).cache()
    validation = prepare_base(spark.read.parquet(args.validation)).cache()
    test = prepare_base(spark.read.parquet(args.test)).cache()

    train_binary = add_class_weights(train, "label_binary", "weight_binary")
    train_multi = add_class_weights(train, "label_original", "weight_multiclass")

    binary_pipeline = build_pipeline("label_binary", "weight_binary", args, "pred_binary_sparkml")
    multiclass_pipeline = build_pipeline("label_original", "weight_multiclass", args, "pred_multiclass_sparkml")

    binary_model = binary_pipeline.fit(train_binary)
    multiclass_model = multiclass_pipeline.fit(train_multi)

    binary_test_predictions = binary_model.transform(test)
    multiclass_test_predictions = multiclass_model.transform(test)

    binary_metrics, binary_confusion, binary_per_class = evaluate_model(
        binary_test_predictions,
        "label_binary",
        "pred_binary_sparkml",
        [0, 1],
    )
    multiclass_metrics, multiclass_confusion, multiclass_per_class = evaluate_model(
        multiclass_test_predictions,
        "label_original",
        "pred_multiclass_sparkml",
        [0, 1, 2, 3],
    )

    binary_model.save(args.binary_model_output)
    multiclass_model.save(args.multiclass_model_output)

    report_output = args.report_output.rstrip("/") + "/"
    binary_confusion_rows = [(a, p, c) for (a, p), c in sorted(binary_confusion.items())]
    spark.createDataFrame(binary_confusion_rows, ["label", "prediction", "count"]).coalesce(1).write.mode("error").option("header", "true").csv(report_output + "binary_confusion_matrix/")

    multiclass_confusion_rows = [(a, p, c) for (a, p), c in sorted(multiclass_confusion.items())]
    spark.createDataFrame(multiclass_confusion_rows, ["label", "prediction", "count"]).coalesce(1).write.mode("error").option("header", "true").csv(report_output + "multiclass_confusion_matrix/")

    summary = {
        "train_distribution_original": distribution(train, "label_original"),
        "validation_distribution_original": distribution(validation, "label_original"),
        "test_distribution_original": distribution(test, "label_original"),
        "train_distribution_binary": distribution(train, "label_binary"),
        "test_distribution_binary": distribution(test, "label_binary"),
        "binary_metrics": binary_metrics,
        "multiclass_metrics": multiclass_metrics,
        "binary_per_class": binary_per_class,
        "multiclass_per_class": multiclass_per_class,
        "label_mapping": LABEL_NAMES,
        "binary_mapping": BINARY_NAMES,
    }
    write_text_file(spark, report_output + "metrics_summary.json", json.dumps(summary, indent=2, sort_keys=True))

    lines = [
        "# Spark ML OffendES Training",
        "",
        "## Inputs",
        "",
        "- Train: `" + args.train + "`",
        "- Validation: `" + args.validation + "`",
        "- Test: `" + args.test + "`",
        "",
        "## Label mapping",
        "",
        "- Binario: `labels 0 y 1 = ofensivo`, `labels 2 y 3 = no_ofensivo`.",
        "- Multiclase: `0=ofensivo_directo`, `1=odio_agresion_grupal`, `2=neutral_no_ofensivo`, `3=vulgaridad_contextual`.",
        "",
        "## Dataset distributions",
        "",
        "```json",
        json.dumps({
            "train_original": summary["train_distribution_original"],
            "validation_original": summary["validation_distribution_original"],
            "test_original": summary["test_distribution_original"],
            "train_binary": summary["train_distribution_binary"],
            "test_binary": summary["test_distribution_binary"],
        }, indent=2, sort_keys=True),
        "```",
        "",
    ]
    lines.extend(markdown_table_metrics("Binary test metrics", binary_metrics))
    lines.extend(markdown_table_metrics("Multiclass test metrics", multiclass_metrics))
    lines.extend([
        "## Outputs",
        "",
        "- Binary model: `" + args.binary_model_output + "`",
        "- Multiclass model: `" + args.multiclass_model_output + "`",
        "- Report: `" + args.report_output + "`",
        "",
        "## Nota",
        "",
        "Este entrenamiento usa `pyspark.ml` nativo. No usa `joblib` ni `sklearn`.",
    ])
    write_text_file(spark, report_output + "summary.md", "\n".join(lines) + "\n")

    print("BINARY_MODEL=" + args.binary_model_output)
    print("MULTICLASS_MODEL=" + args.multiclass_model_output)
    print("REPORT=" + args.report_output)
    print("BINARY_METRICS=" + json.dumps(binary_metrics, sort_keys=True))
    print("MULTICLASS_METRICS=" + json.dumps(multiclass_metrics, sort_keys=True))

    train.unpersist()
    validation.unpersist()
    test.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
