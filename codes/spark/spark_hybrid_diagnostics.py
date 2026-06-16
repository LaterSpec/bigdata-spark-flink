#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Diagnose Spark ML + local rules hybrid output."""

from __future__ import print_function

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_INPUT = "s3://figuretibucket/output/batch/predictions/run_hybrid_sparkml_rules_full/"
DEFAULT_REPORT = "s3://figuretibucket/output/batch/reports/hybrid_diagnostics"
DEFAULT_OUTPUT = "s3://figuretibucket/output/batch/predictions/run_hybrid_scored_full"

FLAGS = [
    "has_terruqueo",
    "has_fraude",
    "has_polarization_signal",
    "has_discriminatory_language",
    "has_general_insult",
]

ALL_LOCAL_FLAGS = [
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
]


def parse_args():
    parser = argparse.ArgumentParser(description="Hybrid diagnostics for Spark ML + rules.")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--report-output", default=DEFAULT_REPORT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
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


def bool_col(name):
    return F.lower(F.coalesce(F.col(name).cast("string"), F.lit("false"))).isin(["true", "1", "yes"])


def add_diagnostic_columns(df):
    for flag in ALL_LOCAL_FLAGS:
        if flag in df.columns:
            df = df.withColumn(flag, bool_col(flag))
        else:
            df = df.withColumn(flag, F.lit(False))

    df = df.withColumn("confidence_binary_sparkml", F.col("confidence_binary_sparkml").cast("double"))
    df = df.withColumn("local_risk_score", F.coalesce(F.col("local_risk_score").cast("int"), F.lit(0)))
    df = df.withColumn("pred_binary_sparkml", F.col("pred_binary_sparkml").cast("int"))
    df = df.withColumn("pred_multiclass_sparkml", F.col("pred_multiclass_sparkml").cast("int"))

    df = df.withColumn(
        "confidence_binary_bucket",
        F.when((F.col("confidence_binary_sparkml") >= 0.5) & (F.col("confidence_binary_sparkml") < 0.6), "0.50-0.60")
        .when((F.col("confidence_binary_sparkml") >= 0.6) & (F.col("confidence_binary_sparkml") < 0.7), "0.60-0.70")
        .when((F.col("confidence_binary_sparkml") >= 0.7) & (F.col("confidence_binary_sparkml") < 0.8), "0.70-0.80")
        .when((F.col("confidence_binary_sparkml") >= 0.8) & (F.col("confidence_binary_sparkml") < 0.9), "0.80-0.90")
        .when((F.col("confidence_binary_sparkml") >= 0.9) & (F.col("confidence_binary_sparkml") <= 1.0), "0.90-1.00")
        .otherwise("out_of_range"),
    )

    strong_rules = (
        F.col("has_terruqueo")
        | F.col("has_fraude")
        | F.col("has_polarization_signal")
        | F.col("has_discriminatory_language")
        | F.col("has_general_insult")
    )
    any_local_rule = None
    for flag in ALL_LOCAL_FLAGS:
        any_local_rule = F.col(flag) if any_local_rule is None else (any_local_rule | F.col(flag))

    df = df.withColumn("has_any_local_rule", any_local_rule)
    df = df.withColumn("has_strong_local_rule", strong_rules)
    df = df.withColumn(
        "ml_offensive_high_confidence",
        (F.col("pred_binary_sparkml") == F.lit(1)) & (F.col("confidence_binary_sparkml") >= F.lit(0.70)),
    )

    df = df.withColumn(
        "hybrid_risk_level",
        F.when(
            (F.col("ml_offensive_high_confidence") & strong_rules)
            | (F.col("local_risk_score") >= 5),
            F.lit("alto"),
        )
        .when(
            (F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") >= 0.60)
            | (F.col("local_risk_score") >= 2),
            F.lit("medio"),
        )
        .otherwise(F.lit("bajo")),
    )

    df = df.withColumn(
        "hybrid_risk_reason",
        F.when(
            F.col("ml_offensive_high_confidence") & strong_rules,
            F.lit("ML ofensivo alta confianza + reglas locales fuertes"),
        )
        .when(
            F.col("ml_offensive_high_confidence") & (~F.col("has_any_local_rule")),
            F.lit("ML ofensivo alta confianza sin reglas locales; revisar posible ofensividad general OffendES"),
        )
        .when(
            (F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") < 0.70) & (~F.col("has_any_local_rule")),
            F.lit("ML ofensivo baja/media confianza sin reglas; no tomar como final"),
        )
        .when(
            (F.col("pred_binary_sparkml") == 0) & (F.col("local_risk_score") >= 3),
            F.lit("Modelo no ofensivo pero reglas locales elevadas; contexto peruano relevante"),
        )
        .when(
            F.col("local_risk_score") >= 2,
            F.lit("Reglas locales activas"),
        )
        .when(
            F.col("pred_binary_sparkml") == 1,
            F.lit("ML ofensivo sin evidencia local fuerte"),
        )
        .otherwise(F.lit("Bajo riesgo hibrido")),
    )
    return df


def pct(count_value, total):
    return (float(count_value) / float(total) * 100.0) if total else 0.0


def table_from_rows(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(item) for item in row) + " |")
    return lines


def collect_distribution(df, col_name, total):
    rows = df.groupBy(col_name).count().orderBy(F.desc("count")).collect()
    return [(r[col_name], int(r["count"]), round(pct(int(r["count"]), total), 4)) for r in rows]


def collect_cross_counts(df, group_cols):
    rows = df.groupBy(*group_cols).count().orderBy(*group_cols).collect()
    out = []
    for r in rows:
        out.append(tuple([r[c] for c in group_cols] + [int(r["count"])]))
    return out


def sample_examples(df, condition, order_cols, limit=10):
    cols = [
        "message_raw",
        "pred_binary_sparkml_label",
        "confidence_binary_sparkml",
        "pred_multiclass_sparkml_label",
        "confidence_multiclass_sparkml",
        "local_risk_score",
        "local_rule_tags",
        "hybrid_risk_level",
        "hybrid_risk_reason",
    ]
    existing = [c for c in cols if c in df.columns]
    result = df.filter(condition).select(*existing)
    for col_name, direction in order_cols:
        result = result.orderBy(F.desc(col_name) if direction == "desc" else F.asc(col_name))
    return result.limit(limit).collect()


def examples_markdown(title, rows):
    lines = ["### " + title, ""]
    if not rows:
        lines.append("Sin ejemplos disponibles.")
        lines.append("")
        return lines
    for i, row in enumerate(rows, 1):
        data = row.asDict()
        msg = str(data.get("message_raw", "")).replace("\n", " ").replace("|", "/")
        if len(msg) > 220:
            msg = msg[:217] + "..."
        lines.append("{}. `{}`".format(i, msg))
        lines.append("")
        lines.append("   - binary: `{}` conf `{}`".format(data.get("pred_binary_sparkml_label"), round(float(data.get("confidence_binary_sparkml") or 0), 4)))
        lines.append("   - multiclass: `{}` conf `{}`".format(data.get("pred_multiclass_sparkml_label"), round(float(data.get("confidence_multiclass_sparkml") or 0), 4)))
        lines.append("   - local_risk_score: `{}` tags: `{}`".format(data.get("local_risk_score"), data.get("local_rule_tags")))
        lines.append("   - hybrid: `{}` - {}".format(data.get("hybrid_risk_level"), data.get("hybrid_risk_reason")))
        lines.append("")
    return lines


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("hybrid-diagnostics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    assert_absent(spark, args.report_output)
    assert_absent(spark, args.output)

    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", "\"")
        .option("escape", "\\")
        .csv(args.input)
    )
    df = add_diagnostic_columns(df).cache()
    total = df.count()

    df.coalesce(args.coalesce).write.mode("error").option("header", "true").csv(args.output)

    binary_dist = collect_distribution(df, "pred_binary_sparkml_label", total)
    multiclass_dist = collect_distribution(df, "pred_multiclass_sparkml_label", total)
    confidence_buckets = collect_distribution(df, "confidence_binary_bucket", total)
    risk_cross = collect_cross_counts(df, ["pred_binary_sparkml_label", "local_risk_score"])

    flag_crosses = {
        flag: collect_cross_counts(df, ["pred_binary_sparkml_label", flag])
        for flag in FLAGS
    }

    offensive_no_rules = df.filter((F.col("pred_binary_sparkml") == 1) & (~F.col("has_any_local_rule"))).count()
    offensive_no_strong_rules = df.filter((F.col("pred_binary_sparkml") == 1) & (~F.col("has_strong_local_rule"))).count()

    examples_high_rules = sample_examples(
        df,
        (F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") >= 0.80) & F.col("has_strong_local_rule"),
        [("local_risk_score", "desc"), ("confidence_binary_sparkml", "desc")],
    )
    examples_high_no_rules = sample_examples(
        df,
        (F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") >= 0.80) & (~F.col("has_any_local_rule")),
        [("confidence_binary_sparkml", "desc")],
    )
    examples_low_no_rules = sample_examples(
        df,
        (F.col("pred_binary_sparkml") == 1) & (F.col("confidence_binary_sparkml") < 0.60) & (~F.col("has_any_local_rule")),
        [("confidence_binary_sparkml", "asc")],
    )
    examples_nonoffensive_high_rules = sample_examples(
        df,
        (F.col("pred_binary_sparkml") == 0) & (F.col("local_risk_score") >= 4),
        [("local_risk_score", "desc")],
    )

    lines = [
        "# Hybrid Diagnostics - Spark ML OffendES + Rules v1",
        "",
        "## Inputs",
        "",
        "- Hybrid input: `" + args.input + "`",
        "- Scored output: `" + args.output + "`",
        "- Total procesado: `" + str(total) + "`",
        "",
        "## Nota de interpretacion",
        "",
        "El `53.24%` ofensivo de Spark ML no se toma como metrica final. Este diagnostico evalua si la prediccion ML esta alineada con reglas locales y niveles de confianza.",
        "",
        "## Distribucion pred_binary_sparkml",
        "",
    ]
    lines.extend(table_from_rows(["label", "count", "percentage"], binary_dist))
    lines.extend(["", "## Distribucion pred_multiclass_sparkml", ""])
    lines.extend(table_from_rows(["label", "count", "percentage"], multiclass_dist))
    lines.extend(["", "## Buckets confidence_binary_sparkml", ""])
    lines.extend(table_from_rows(["bucket", "count", "percentage"], confidence_buckets))
    lines.extend(["", "## Cruce pred_binary_sparkml x local_risk_score", ""])
    lines.extend(table_from_rows(["pred_binary", "local_risk_score", "count"], risk_cross))
    lines.extend(["", "## Cruce pred_binary_sparkml x flags locales", ""])
    for flag, rows in flag_crosses.items():
        lines.extend(["", "### " + flag, ""])
        lines.extend(table_from_rows(["pred_binary", flag, "count"], rows))
    lines.extend([
        "",
        "## Ofensivos ML sin reglas locales",
        "",
        "- Ofensivos ML sin ninguna regla local activa: `{}` (`{}%`)".format(offensive_no_rules, round(pct(offensive_no_rules, total), 4)),
        "- Ofensivos ML sin reglas fuertes locales: `{}` (`{}%`)".format(offensive_no_strong_rules, round(pct(offensive_no_strong_rules, total), 4)),
        "",
        "## Nuevas columnas",
        "",
        "- `ml_offensive_high_confidence`: ofensivo Spark ML con `confidence_binary_sparkml >= 0.70`.",
        "- `hybrid_risk_level`: `bajo`, `medio`, `alto` combinando confianza ML y reglas.",
        "- `hybrid_risk_reason`: explicacion textual basada en ML + reglas.",
        "",
    ])
    lines.extend(examples_markdown("Ofensivo alta confianza + reglas fuertes", examples_high_rules))
    lines.extend(examples_markdown("Ofensivo alta confianza + sin reglas", examples_high_no_rules))
    lines.extend(examples_markdown("Ofensivo baja confianza + sin reglas", examples_low_no_rules))
    lines.extend(examples_markdown("No ofensivo + local_risk_score alto", examples_nonoffensive_high_rules))

    report_path = args.report_output.rstrip("/") + "/summary.md"
    write_text_file(spark, report_path, "\n".join(lines) + "\n")

    print("TOTAL_ROWS=" + str(total))
    print("REPORT=" + report_path)
    print("SCORED_OUTPUT=" + args.output)
    print("OFFENSIVE_NO_RULES=" + str(offensive_no_rules))
    print("OFFENSIVE_NO_STRONG_RULES=" + str(offensive_no_strong_rules))

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
