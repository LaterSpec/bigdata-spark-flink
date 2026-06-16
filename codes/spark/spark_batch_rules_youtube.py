#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Spark Batch rules job for Peruvian YouTube Live Chat comments.

This first EMR phase intentionally uses Spark-native expressions only. It does
not load sklearn/joblib artifacts; the ML model will be ported in a later phase.
"""

from __future__ import print_function

import argparse
import re
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_BUCKET = "s3://figuretibucket"
DEFAULT_INPUT = DEFAULT_BUCKET + "/data/raw/youtube/youtube_lake.csv"
DEFAULT_OUTPUT_PREDICTIONS = DEFAULT_BUCKET + "/output/batch/predictions"
DEFAULT_OUTPUT_AGGREGATES = DEFAULT_BUCKET + "/output/batch/aggregates_by_minute"
DEFAULT_OUTPUT_REPORTS = DEFAULT_BUCKET + "/output/batch/reports"


TERRUQUEO_TERMS = [
    "terruco", "terruqueo", "senderista", "rojo", "comunista",
    "terruca", "terrucos", "terrucada", "terruquear", "terruquean",
    "senderistas", "sendero", "sendero luminoso", "emerretista", "mrta",
    "movadef", "roja", "rojos", "comunistas", "comunacho",
    "comunachos", "caviares",
]

FRAUDE_TERMS = [
    "fraude", "robo", "fraudulento", "fraudulenta", "fraudearon",
    "robaron votos", "robo electoral", "actas falsas", "actas perdidas",
    "actas duplicadas", "actas impugnadas", "mesas anuladas",
    "anular mesas", "votos inflados", "votos perdidos",
    "conteo manipulado", "conteo lento", "trampa electoral",
    "irregularidades", "nulidad", "impugnacion", "impugnación",
]

ELECTORAL_INSTITUTION_TERMS = [
    "onpe", "jne", "reniec", "personero", "personeros", "actas",
    "mesa", "mesas", "cedula", "cédula", "padron", "padrón",
    "votos", "conteo", "boca de urna", "flash electoral",
]

POLITICAL_TERMS = [
    "keiko", "fujimori", "fuerza popular", "fp", "roberto sánchez",
    "roberto sanchez", "sánchez", "sanchez", "jp", "juntos por el perú",
    "juntos por el peru", "castillo", "perú libre", "peru libre",
    "reniec", "vizcarra", "fujimorismo", "antifujimorismo",
    "pedro castillo", "lapiz", "lápiz", "lapicito", "cerron", "cerrón",
    "dina", "boluarte", "ppk", "acuña", "cesar acuña", "césar acuña",
    "app", "apra", "aprista", "lopez aliaga", "lópez aliaga",
    "rafael lopez aliaga", "rafael lópez aliaga", "rla", "porky",
    "renovacion popular", "renovación popular", "avanza pais",
    "avanza país", "antauro", "humala",
]

POLARIZATION_TERMS = [
    "zurdo", "zurdos", "caviar", "vendepatria", "corrupto",
    "corruptos", "dictadura", "terrorista", "odio", "lacra",
    "traidor", "mafiosa", "mafioso", "golpista", "zurda",
    "izquierdista", "izquierdistas", "derecha bruta", "dba", "facho",
    "facha", "fachos", "fascista", "fascistas", "vendepatrias",
    "corrupta", "corruptas", "dictador", "terroristas", "lacras",
    "traidora", "traidores", "mafia", "delincuente", "delincuentes",
    "criminal", "criminales", "rata", "ratas", "basura", "escoria",
]

ETHNIC_RACIAL_DISCRIMINATION_TERMS = [
    "serrano", "serrana", "serranos", "serranas", "serranazo",
    "serranaza", "serranear", "cholo", "chola", "cholos", "cholas",
    "cholada", "cholear", "indio", "india", "indios", "indias",
    "indigena", "indígena", "paisano", "paisana", "provinciano",
    "provinciana", "ignorante de la sierra", "bajado del cerro",
    "llama", "llamas", "auquenido", "auquénido", "motoso", "mote",
]

HOMOPHOBIC_SLUR_TERMS = [
    "cabro", "cabros", "kbro", "kbros", "kbraso", "kbron", "kbrón",
    "cabron", "cabrón", "cabra", "cabrita", "maricon", "maricón",
    "marica", "rosquete", "rosquetes", "loca",
]

GENERAL_INSULT_TERMS = [
    "csm", "ctm", "mierda", "pendejo", "pendeja", "pendejos",
    "pendejas", "idiota", "estupido", "estúpido", "burro", "bruto",
    "imbecil", "imbécil", "ignorante", "vago", "vaga", "payaso",
    "payasa", "miserable", "asqueroso", "asquerosa", "porqueria",
    "porquería",
]

FLAG_COLUMNS = [
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
    parser = argparse.ArgumentParser(description="Spark batch rules for YouTube comments.")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-predictions", default=DEFAULT_OUTPUT_PREDICTIONS)
    parser.add_argument("--output-aggregates", default=DEFAULT_OUTPUT_AGGREGATES)
    parser.add_argument("--output-reports", default=DEFAULT_OUTPUT_REPORTS)
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit. 0 means all rows.")
    parser.add_argument("--run-id", default=None, help="Versioned run id. Example: run_rules_v1_5000")
    parser.add_argument("--coalesce", type=int, default=1, help="Output partitions per result dataset.")
    return parser.parse_args()


def normalize_term(term):
    term = term.lower()
    accents = {
        u"á": "a", u"é": "e", u"í": "i", u"ó": "o", u"ú": "u",
        u"ü": "u", u"ñ": "n",
    }
    for src, dst in accents.items():
        term = term.replace(src, dst)
    term = re.sub(r"(.)\1{2,}", r"\1", term)
    term = re.sub(r"[^a-z0-9:_\s]", " ", term)
    term = re.sub(r"\s+", " ", term).strip()
    return term


def terms_regex(terms):
    normalized = sorted(set(normalize_term(term) for term in terms if normalize_term(term)))
    escaped = []
    for term in normalized:
        parts = [re.escape(part) for part in term.split()]
        escaped.append(r"\s+".join(parts))
    return r"(?i)(^|[^A-Za-z0-9_])(" + "|".join(escaped) + r")($|[^A-Za-z0-9_])"


def clean_text_expr(col):
    text = F.lower(F.coalesce(col.cast("string"), F.lit("")))
    text = F.regexp_replace(text, r"https?://\S+|www\.\S+", " ")
    text = F.regexp_replace(text, r"[\r\n\t]+", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def norm_text_expr(col):
    text = clean_text_expr(col)
    text = F.translate(text, u"áéíóúüñÁÉÍÓÚÜÑ", "aeiouunAEIOUUN")
    # Avoid Java regex backreferences here; they can fail on emoji/surrogate pairs.
    text = F.regexp_replace(text, r"a{3,}", "a")
    text = F.regexp_replace(text, r"e{3,}", "e")
    text = F.regexp_replace(text, r"i{3,}", "i")
    text = F.regexp_replace(text, r"o{3,}", "o")
    text = F.regexp_replace(text, r"u{3,}", "u")
    text = F.regexp_replace(text, r"[^a-z0-9:_\s]", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def add_rules(df):
    df = df.withColumn("message_raw", F.coalesce(F.col("message").cast("string"), F.lit("")))
    df = df.withColumn("message_clean", clean_text_expr(F.col("message_raw")))
    df = df.withColumn("text_norm", norm_text_expr(F.col("message_raw")))

    terruqueo = F.col("text_norm").rlike(terms_regex(TERRUQUEO_TERMS))
    fraude = F.col("text_norm").rlike(terms_regex(FRAUDE_TERMS))
    electoral = F.col("text_norm").rlike(terms_regex(ELECTORAL_INSTITUTION_TERMS))
    political = F.col("text_norm").rlike(terms_regex(POLITICAL_TERMS))
    polarization_terms = F.col("text_norm").rlike(terms_regex(POLARIZATION_TERMS))
    ethnic = F.col("text_norm").rlike(terms_regex(ETHNIC_RACIAL_DISCRIMINATION_TERMS))
    homophobic = F.col("text_norm").rlike(terms_regex(HOMOPHOBIC_SLUR_TERMS))
    general_insult = F.col("text_norm").rlike(terms_regex(GENERAL_INSULT_TERMS))

    compact = F.regexp_replace(F.col("text_norm"), r"\s+", "")
    raw_no_emoji = F.regexp_replace(F.col("message_raw"), u"[\U0001F300-\U0001FAFF\u2600-\u27BF]", "")
    raw_no_space = F.regexp_replace(raw_no_emoji, r"\s+", "")
    emoji_or_symbol_only = (
        (F.length(F.trim(F.col("message_raw"))) > 0)
        & (
            (F.length(raw_no_space) == 0)
            | raw_no_space.rlike(r"^[\W_]+$")
        )
    )
    repeated_noise = F.col("text_norm").rlike(r"(?i)((ja){5,}|(jaja){3,}|(xd){4,}|(zzz){3,})")
    spam = (
        (F.length(compact) == 0)
        | (F.length(compact) <= 2)
        | emoji_or_symbol_only
        | repeated_noise
    )

    df = df.withColumn("has_terruqueo", terruqueo)
    df = df.withColumn("has_fraude", fraude)
    df = df.withColumn("has_electoral_institution", electoral)
    df = df.withColumn("has_political_mention", political)
    df = df.withColumn("has_ethnic_racial_slur", ethnic)
    df = df.withColumn("has_homophobic_slur", homophobic)
    df = df.withColumn("has_general_insult", general_insult)
    df = df.withColumn("has_discriminatory_language", ethnic | homophobic)
    df = df.withColumn(
        "has_polarization_signal",
        political & (polarization_terms | terruqueo | fraude | general_insult | ethnic | homophobic),
    )
    df = df.withColumn("is_spam_noise", spam)

    score = (
        F.when(F.col("has_terruqueo"), F.lit(2)).otherwise(F.lit(0))
        + F.when(F.col("has_discriminatory_language"), F.lit(2)).otherwise(F.lit(0))
        + F.when(F.col("has_homophobic_slur"), F.lit(2)).otherwise(F.lit(0))
        + F.when(F.col("has_fraude"), F.lit(2)).otherwise(F.lit(0))
        + F.when(F.col("has_political_mention"), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("has_polarization_signal"), F.lit(1)).otherwise(F.lit(0))
    )
    strong_signal = (
        F.col("has_terruqueo")
        | F.col("has_discriminatory_language")
        | F.col("has_homophobic_slur")
        | F.col("has_fraude")
    )
    score = score - F.when(F.col("is_spam_noise") & (~strong_signal), F.lit(1)).otherwise(F.lit(0))
    df = df.withColumn("local_risk_score", F.greatest(score, F.lit(0)))

    tag_array = F.array(
        F.when(F.col("has_terruqueo"), F.lit("terruqueo")),
        F.when(F.col("has_fraude"), F.lit("fraude")),
        F.when(F.col("has_electoral_institution"), F.lit("electoral_institution")),
        F.when(F.col("has_political_mention"), F.lit("political_mention")),
        F.when(F.col("has_polarization_signal"), F.lit("polarization")),
        F.when(F.col("has_ethnic_racial_slur"), F.lit("ethnic_racial_slur")),
        F.when(F.col("has_homophobic_slur"), F.lit("homophobic_slur")),
        F.when(F.col("has_general_insult"), F.lit("general_insult")),
        F.when(F.col("is_spam_noise"), F.lit("spam_noise")),
    )
    df = df.withColumn("local_rule_tags", F.concat_ws("|", tag_array))

    df = df.withColumn(
        "minute",
        F.when(
            F.col("video_offset_msec").cast("double").isNotNull(),
            F.floor(F.col("video_offset_msec").cast("double") / F.lit(60000)).cast("long"),
        ),
    )
    return df


def output_path(base_path, run_id):
    return base_path.rstrip("/") + "/" + run_id + "/"


def assert_output_absent(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    if fs.exists(hadoop_path):
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


def pct_expr(numerator_col, total_rows):
    if total_rows == 0:
        return F.lit(0.0)
    return F.round((F.col(numerator_col).cast("double") / F.lit(float(total_rows))) * F.lit(100.0), 4)


def collect_global_summary(df, total_rows):
    flag_sums = [
        F.sum(F.when(F.col(flag), F.lit(1)).otherwise(F.lit(0))).alias(flag + "_count")
        for flag in FLAG_COLUMNS
    ]
    agg_row = df.agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.avg("local_risk_score").alias("avg_local_risk_score"),
        F.min("local_risk_score").alias("min_local_risk_score"),
        F.max("local_risk_score").alias("max_local_risk_score"),
        *flag_sums
    ).collect()[0].asDict()

    rows = []
    for flag in FLAG_COLUMNS:
        count_value = int(agg_row.get(flag + "_count") or 0)
        pct_value = (float(count_value) / float(total_rows) * 100.0) if total_rows else 0.0
        rows.append((flag, count_value, round(pct_value, 4)))
    return agg_row, rows


def main():
    args = parse_args()
    run_id = args.run_id or ("run_rules_v1_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S"))

    spark = (
        SparkSession.builder
        .appName("spark-batch-rules-youtube-" + run_id)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    pred_path = output_path(args.output_predictions, run_id)
    agg_path = output_path(args.output_aggregates, run_id)
    report_path = output_path(args.output_reports, run_id)
    assert_output_absent(spark, pred_path)
    assert_output_absent(spark, agg_path)
    assert_output_absent(spark, report_path)

    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv(args.input)
    )
    if args.limit and args.limit > 0:
        df = df.limit(args.limit)

    rules_df = add_rules(df).cache()
    total_rows = rules_df.count()

    output_columns = [
        "source_file",
        "video_id",
        "timestamp_text",
        "timestamp_usec",
        "video_offset_msec",
        "minute",
        "author",
        "message_raw",
        "message_clean",
        "text_norm",
    ] + FLAG_COLUMNS + [
        "local_risk_score",
        "local_rule_tags",
    ]
    existing_output_columns = [col for col in output_columns if col in rules_df.columns]
    rules_df.select(*existing_output_columns).coalesce(args.coalesce).write.mode("error").option("header", "true").csv(pred_path)

    aggregates = (
        rules_df
        .groupBy("source_file", "video_id", "minute")
        .agg(
            F.count(F.lit(1)).alias("comentarios"),
            F.sum(F.when(F.col("has_terruqueo"), F.lit(1)).otherwise(F.lit(0))).alias("terruqueo"),
            F.sum(F.when(F.col("has_fraude"), F.lit(1)).otherwise(F.lit(0))).alias("fraude"),
            F.sum(F.when(F.col("has_political_mention"), F.lit(1)).otherwise(F.lit(0))).alias("menciones_politicas"),
            F.sum(F.when(F.col("has_polarization_signal"), F.lit(1)).otherwise(F.lit(0))).alias("polarizacion"),
            F.sum(F.when(F.col("has_discriminatory_language"), F.lit(1)).otherwise(F.lit(0))).alias("lenguaje_discriminatorio"),
            F.sum(F.when(F.col("is_spam_noise"), F.lit(1)).otherwise(F.lit(0))).alias("spam"),
            F.avg("local_risk_score").alias("avg_local_risk_score"),
            F.max("local_risk_score").alias("max_local_risk_score"),
        )
        .orderBy("source_file", "video_id", "minute")
    )
    aggregates.coalesce(args.coalesce).write.mode("error").option("header", "true").csv(agg_path)

    agg_row, flag_rows = collect_global_summary(rules_df, total_rows)
    summary_df = spark.createDataFrame(flag_rows, ["flag", "count", "percentage"])
    summary_df.coalesce(1).write.mode("error").option("header", "true").csv(report_path + "flag_summary_csv/")

    score_stats_df = spark.createDataFrame([
        (
            int(total_rows),
            float(agg_row.get("avg_local_risk_score") or 0.0),
            int(agg_row.get("min_local_risk_score") or 0),
            int(agg_row.get("max_local_risk_score") or 0),
        )
    ], ["total_rows", "avg_local_risk_score", "min_local_risk_score", "max_local_risk_score"])
    score_stats_df.coalesce(1).write.mode("error").option("header", "true").csv(report_path + "score_stats_csv/")

    top_risk = (
        rules_df
        .select("local_risk_score", "local_rule_tags", "message_raw")
        .orderBy(F.desc("local_risk_score"))
        .limit(20)
    )
    top_risk.coalesce(1).write.mode("error").option("header", "true").csv(report_path + "top_risk_comments_csv/")

    report_lines = [
        "# Spark Batch Rules Report - " + run_id,
        "",
        "- Input: `" + args.input + "`",
        "- Limit: `" + str(args.limit if args.limit else "all") + "`",
        "- Total procesado: `" + str(total_rows) + "`",
        "- Predictions: `" + pred_path + "`",
        "- Aggregates by minute: `" + agg_path + "`",
        "",
        "## Local Flags",
        "",
        "| flag | count | percentage |",
        "|---|---:|---:|",
    ]
    for flag, count_value, pct_value in flag_rows:
        report_lines.append("| " + flag + " | " + str(count_value) + " | " + str(pct_value) + "% |")
    report_lines.extend([
        "",
        "## Risk Score",
        "",
        "- Promedio: `" + str(round(float(agg_row.get("avg_local_risk_score") or 0.0), 4)) + "`",
        "- Minimo: `" + str(int(agg_row.get("min_local_risk_score") or 0)) + "`",
        "- Maximo: `" + str(int(agg_row.get("max_local_risk_score") or 0)) + "`",
        "",
        "## Nota",
        "",
        "Este job usa reglas Spark puras. No usa sklearn/joblib. La capa ML con pyspark.ml se implementara en una fase posterior.",
    ])
    write_text_file(spark, report_path + "summary.md", "\n".join(report_lines) + "\n")

    print("RUN_ID=" + run_id)
    print("TOTAL_ROWS=" + str(total_rows))
    print("PREDICTIONS=" + pred_path)
    print("AGGREGATES=" + agg_path)
    print("REPORT=" + report_path)

    rules_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
