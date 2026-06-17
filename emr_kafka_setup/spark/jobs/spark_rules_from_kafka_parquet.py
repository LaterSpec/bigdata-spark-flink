#!/usr/bin/env python3
import argparse
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TERRUQUEO_TERMS = [
    "terruco", "terruqueo", "senderista", "rojo", "comunista", "terruca",
    "terrucos", "sendero luminoso", "movadef", "caviares",
]
FRAUDE_TERMS = [
    "fraude", "robo", "fraudulento", "fraudulenta", "robaron votos",
    "robo electoral", "actas falsas", "actas perdidas", "actas duplicadas",
    "actas impugnadas", "mesas anuladas", "votos inflados",
    "conteo manipulado", "irregularidades", "nulidad", "impugnacion",
    "impugnación",
]
ELECTORAL_INSTITUTION_TERMS = [
    "onpe", "jne", "reniec", "personero", "personeros", "actas", "mesa",
    "mesas", "cedula", "cédula", "padron", "padrón", "votos", "conteo",
]
POLITICAL_TERMS = [
    "keiko", "fujimori", "fuerza popular", "fp", "roberto sánchez",
    "roberto sanchez", "sánchez", "sanchez", "jp", "juntos por el perú",
    "juntos por el peru", "castillo", "perú libre", "peru libre",
    "fujimorismo", "antifujimorismo",
]
POLARIZATION_TERMS = [
    "zurdo", "zurdos", "caviar", "vendepatria", "corrupto", "corruptos",
    "dictadura", "terrorista", "lacra", "traidor", "mafiosa", "mafioso",
    "golpista", "facho", "fascista", "mafia", "delincuente", "criminal",
    "rata", "basura", "escoria",
]
ETHNIC_RACIAL_DISCRIMINATION_TERMS = [
    "serrano", "serrana", "serranos", "serranas", "cholo", "chola",
    "cholos", "cholas", "indio", "india", "indios", "indias", "indigena",
    "indígena", "paisano", "provinciano", "bajado del cerro", "llama",
    "auquenido", "motoso",
]
HOMOPHOBIC_SLUR_TERMS = [
    "cabro", "cabros", "kbro", "kbros", "cabron", "cabrón", "maricon",
    "maricón", "marica", "rosquete", "loca",
]
GENERAL_INSULT_TERMS = [
    "csm", "ctm", "mierda", "pendejo", "pendeja", "idiota", "estupido",
    "estúpido", "burro", "bruto", "imbecil", "imbécil", "ignorante",
    "payaso", "miserable", "porqueria", "porquería",
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

IDENTITY_COLUMNS = [
    "event_id", "row_number", "source_s3_path", "kafka_topic", "kafka_partition",
    "kafka_offset", "kafka_timestamp", "source_file", "video_id",
    "timestamp_text", "timestamp_usec", "video_offset_msec", "author",
    "author_channel_id", "message_raw", "message_clean",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Apply local Peruvian rules to Kafka-origin parquet.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report-output", required=True)
    parser.add_argument("--coalesce", type=int, default=1)
    return parser.parse_args()


def normalize_term(term):
    term = term.lower()
    for src, dst in {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n"}.items():
        term = term.replace(src, dst)
    term = re.sub(r"[^a-z0-9:_\s]", " ", term)
    return re.sub(r"\s+", " ", term).strip()


def terms_regex(terms):
    normalized = sorted(set(normalize_term(term) for term in terms if normalize_term(term)))
    escaped = [r"\s+".join(re.escape(part) for part in term.split()) for term in normalized]
    return r"(?i)(^|[^A-Za-z0-9_])(" + "|".join(escaped) + r")($|[^A-Za-z0-9_])"


def clean_text_expr(col):
    text = F.lower(F.coalesce(col.cast("string"), F.lit("")))
    text = F.regexp_replace(text, r"https?://\S+|www\.\S+", " ")
    text = F.regexp_replace(text, r"[\r\n\t]+", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def norm_text_expr(col):
    text = clean_text_expr(col)
    text = F.translate(text, "áéíóúüñÁÉÍÓÚÜÑ", "aeiouunAEIOUUN")
    text = F.regexp_replace(text, r"[^a-z0-9:_\s]", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def write_text(spark, path, content):
    jpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(jpath, True)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def add_rules(df):
    text_source = F.coalesce(F.col("message_clean"), F.col("message_raw"), F.lit(""))
    df = df.withColumn("message_raw", F.coalesce(F.col("message_raw"), F.lit("")))
    df = df.withColumn("message_clean", F.coalesce(F.col("message_clean"), F.col("message_raw"), F.lit("")))
    df = df.withColumn("text_norm", norm_text_expr(text_source))

    terruqueo = F.col("text_norm").rlike(terms_regex(TERRUQUEO_TERMS))
    fraude = F.col("text_norm").rlike(terms_regex(FRAUDE_TERMS))
    electoral = F.col("text_norm").rlike(terms_regex(ELECTORAL_INSTITUTION_TERMS))
    political = F.col("text_norm").rlike(terms_regex(POLITICAL_TERMS))
    polarization_terms = F.col("text_norm").rlike(terms_regex(POLARIZATION_TERMS))
    ethnic = F.col("text_norm").rlike(terms_regex(ETHNIC_RACIAL_DISCRIMINATION_TERMS))
    homophobic = F.col("text_norm").rlike(terms_regex(HOMOPHOBIC_SLUR_TERMS))
    general_insult = F.col("text_norm").rlike(terms_regex(GENERAL_INSULT_TERMS))
    compact = F.regexp_replace(F.col("text_norm"), r"\s+", "")
    spam = (F.length(compact) == 0) | (F.length(compact) <= 2) | F.col("text_norm").rlike(r"(?i)((ja){5,}|(jaja){3,}|(xd){4,})")

    df = df.withColumn("has_terruqueo", terruqueo)
    df = df.withColumn("has_fraude", fraude)
    df = df.withColumn("has_electoral_institution", electoral)
    df = df.withColumn("has_political_mention", political)
    df = df.withColumn("has_ethnic_racial_slur", ethnic)
    df = df.withColumn("has_homophobic_slur", homophobic)
    df = df.withColumn("has_general_insult", general_insult)
    df = df.withColumn("has_discriminatory_language", ethnic | homophobic)
    df = df.withColumn("has_polarization_signal", political & (polarization_terms | terruqueo | fraude | general_insult | ethnic | homophobic))
    df = df.withColumn("is_spam_noise", spam)

    score = (
        F.when(F.col("has_terruqueo"), 2).otherwise(0)
        + F.when(F.col("has_fraude"), 2).otherwise(0)
        + F.when(F.col("has_discriminatory_language"), 2).otherwise(0)
        + F.when(F.col("has_homophobic_slur"), 2).otherwise(0)
        + F.when(F.col("has_political_mention"), 1).otherwise(0)
        + F.when(F.col("has_polarization_signal"), 1).otherwise(0)
    )
    df = df.withColumn("local_risk_score", F.greatest(score, F.lit(0)))
    tags = F.array(
        F.when(F.col("has_terruqueo"), "terruqueo"),
        F.when(F.col("has_fraude"), "fraude"),
        F.when(F.col("has_electoral_institution"), "electoral_institution"),
        F.when(F.col("has_political_mention"), "political_mention"),
        F.when(F.col("has_polarization_signal"), "polarization"),
        F.when(F.col("has_ethnic_racial_slur"), "ethnic_racial_slur"),
        F.when(F.col("has_homophobic_slur"), "homophobic_slur"),
        F.when(F.col("has_general_insult"), "general_insult"),
        F.when(F.col("is_spam_noise"), "spam_noise"),
    )
    df = df.withColumn("local_rule_tags", F.concat_ws("|", tags))
    df = df.withColumn("minute", F.when(F.col("video_offset_msec").cast("double").isNotNull(), F.floor(F.col("video_offset_msec").cast("double") / 60000).cast("long")))
    return df


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("rules-from-kafka-parquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    source = spark.read.parquet(args.input)
    total_in = source.count()
    rules = add_rules(source).cache()
    total_out = rules.count()

    output_cols = [c for c in IDENTITY_COLUMNS if c in rules.columns] + ["minute", "text_norm"] + FLAG_COLUMNS + ["local_risk_score", "local_rule_tags"]
    rules.select(*output_cols).coalesce(args.coalesce).write.mode("overwrite").parquet(args.output)

    summary = rules.agg(*[F.sum(F.when(F.col(c), 1).otherwise(0)).alias(c) for c in FLAG_COLUMNS]).collect()[0].asDict()
    lines = [
        "# Job 2 - Reglas Locales desde Kafka Parquet",
        "",
        f"- Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"- Input: `{args.input}`",
        f"- Output: `{args.output}`",
        f"- Input rows: `{total_in}`",
        f"- Output rows: `{total_out}`",
        "",
        "| flag | count | percentage |",
        "|---|---:|---:|",
    ]
    for flag in FLAG_COLUMNS:
        count = int(summary.get(flag) or 0)
        pct = round((count / total_out * 100.0), 4) if total_out else 0.0
        lines.append(f"| {flag} | {count} | {pct}% |")
    write_text(spark, args.report_output.rstrip("/") + "/summary.md", "\n".join(lines) + "\n")

    print(f"INPUT_ROWS={total_in}")
    print(f"OUTPUT_ROWS={total_out}")
    print(f"OUTPUT={args.output}")
    print(f"REPORT={args.report_output.rstrip('/')}/summary.md")
    rules.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

