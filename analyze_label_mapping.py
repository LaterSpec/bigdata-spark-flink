from __future__ import annotations

import json
from pathlib import Path

import polars as pl

DATASET_DIR = Path("dataset")
TRAIN_PATH = DATASET_DIR / "train.parquet"
SAMPLE_JSON_PATH = DATASET_DIR / "train_first_200.json"
REPORT_PATH = DATASET_DIR / "label_audit_report.md"
SAMPLE_SIZE = 200
EXAMPLES_PER_LABEL = 12


def export_first_sample(df: pl.DataFrame) -> pl.DataFrame:
    sample = df.head(SAMPLE_SIZE)
    SAMPLE_JSON_PATH.write_text(sample.write_json(), encoding="utf-8")
    return sample


def build_label_summary(df: pl.DataFrame) -> list[dict]:
    summary = (
        df.with_columns(pl.col("comment").str.len_chars().alias("len_chars"))
        .group_by("label")
        .agg(
            [
                pl.len().alias("count"),
                pl.col("len_chars").mean().round(2).alias("avg_len_chars"),
                pl.col("len_chars").median().alias("median_len_chars"),
            ]
        )
        .sort("label")
    )
    return summary.to_dicts()


def example_rows(df: pl.DataFrame, label: int, n: int = EXAMPLES_PER_LABEL) -> list[dict]:
    return (
        df.filter(pl.col("label") == label)
        .select(["comment_id", "media", "comment"])
        .head(n)
        .to_dicts()
    )


def write_report(df: pl.DataFrame, first_sample: pl.DataFrame) -> None:
    label_summary = build_label_summary(df)
    first_200_counts = (
        first_sample.group_by("label").len().sort("label").to_dicts()
        if first_sample.height
        else []
    )

    inferred_mapping = {
        0: "ofensivo_directo: insulto o ataque claro contra una persona",
        1: "odio_agresion_grupal: agresion intensa, deshumanizacion o ataque a colectivos/temas sensibles",
        2: "neutral_no_ofensivo: comentario normal, apoyo, conversacion o critica no marcada como odio",
        3: "vulgaridad_contextual: profanidad o tono brusco, pero muchas veces sin intencion clara de odio",
    }

    lines: list[str] = []
    lines.append("# Auditoria inicial de labels del dataset externo")
    lines.append("")
    lines.append(f"- Archivo analizado: `{TRAIN_PATH.as_posix()}`")
    lines.append(f"- Filas totales en train: **{df.height}**")
    lines.append(f"- Muestra exportada: `{SAMPLE_JSON_PATH.as_posix()}` con **{first_sample.height}** filas")
    lines.append("")
    lines.append("## Relacion con el plan")
    lines.append("")
    lines.append(
        "Segun `plan_pipeline_bigdata_discurso_politico.md`, este dataset externo debe servir "
        "para entrenar el modelo base de NLP antes de aplicarlo al chat politico peruano. "
        "Por eso lo urgente no es solo leer los numeros, sino fijar un mapeo defendible entre "
        "`label` y categorias de negocio para el modelo."
    )
    lines.append("")
    lines.append("## Distribucion global de labels")
    lines.append("")
    for row in label_summary:
        lines.append(
            f"- `label {row['label']}`: {row['count']} filas, "
            f"promedio {row['avg_len_chars']} caracteres, mediana {row['median_len_chars']}"
        )
    lines.append("")
    lines.append("## Distribucion de las primeras 200 filas")
    lines.append("")
    for row in first_200_counts:
        lines.append(f"- `label {row['label']}`: {row['len']} filas")
    if not first_200_counts:
        lines.append("- Sin filas en la muestra")
    lines.append("")
    lines.append("## Hallazgo clave de la muestra inicial")
    lines.append("")
    lines.append(
        "Las primeras 200 filas del `train` caen completamente en `label = 2`. "
        "Al leer esos textos, predominan comentarios de apoyo, conversacion casual, sugerencias, "
        "humor y critica no claramente discriminatoria. Eso vuelve muy probable que `2` sea la "
        "clase base segura: `neutral` o `no_ofensivo`."
    )
    lines.append("")
    lines.append("## Mapeo provisional recomendado")
    lines.append("")
    for label, meaning in inferred_mapping.items():
        lines.append(f"- `label {label}` -> `{meaning}`")
    lines.append("")
    lines.append("## Como lo usaria para el modelo")
    lines.append("")
    lines.append("- Modelo binario inicial recomendado: `2 = no_ofensivo`, `0/1/3 = ofensivo_o_toxico`.")
    lines.append(
        "- Modelo multicategoria recomendado para analisis: "
        "`2 = neutral`, `3 = vulgar`, `0 = ofensivo_directo`, `1 = odio_o_agresion_grupal`."
    )
    lines.append(
        "- Para el caso peruano del plan, conviene agregar reglas externas para `terruqueo`, "
        "`fraude`, `polarizante` y `spam_ruido`, porque esas clases no aparecen de forma explicita aqui."
    )
    lines.append("")
    lines.append("## Riesgo importante")
    lines.append("")
    lines.append(
        "Este mapeo es inferido por lectura manual del texto, no por una leyenda oficial del dataset. "
        "Se ve consistente, pero antes de entrenar la version final conviene validar al menos 30 a 50 "
        "ejemplos por label."
    )
    lines.append("")
    lines.append("## Ejemplos por label")
    lines.append("")
    for label in sorted(df["label"].unique().to_list()):
        lines.append(f"### Label {label}")
        lines.append("")
        for row in example_rows(df, label):
            comment = row["comment"].replace("\n", " ").strip()
            lines.append(
                f"- `{row['comment_id']}` [{row['media']}]: {comment}"
            )
        lines.append("")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    df = pl.read_parquet(TRAIN_PATH)
    first_sample = export_first_sample(df)
    write_report(df, first_sample)
    print(f"Sample JSON: {SAMPLE_JSON_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
