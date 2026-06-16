from __future__ import annotations

import argparse
from collections import Counter

import joblib
import pandas as pd

from peruvian_rules import apply_rules, get_rule_matches
from utils import ARTIFACTS_DIR, MULTICLASS_MAP, OUTPUTS_DIR, REPORTS_DIR, YOUTUBE_CSV_PATH, clean_text, ensure_directories

BINARY_LABELS = {
    0: "no_ofensivo",
    1: "ofensivo",
}

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inferencia local sobre youtube_lake.csv")
    parser.add_argument("--sample-size", type=int, default=500, help="Numero de filas a muestrear")
    parser.add_argument("--seed", type=int, default=42, help="Semilla para muestreo reproducible")
    return parser.parse_args()


def examples_for(df: pd.DataFrame, flag: str, limit: int = 8) -> list[str]:
    subset = df.loc[df[flag], "message_raw"].head(limit)
    return [str(value).replace("\n", " ").strip() for value in subset]


def top_terms(df: pd.DataFrame) -> dict[str, list[tuple[str, int]]]:
    counters = {
        "terruqueo": Counter(),
        "fraude": Counter(),
        "electoral_institution": Counter(),
        "political_mention": Counter(),
        "polarization": Counter(),
        "ethnic_racial_slur": Counter(),
        "homophobic_slur": Counter(),
        "general_insult": Counter(),
    }

    for message in df["message_raw"]:
        matches = get_rule_matches(str(message))
        for category, terms in matches.items():
            counters[category].update(terms)

    return {category: counter.most_common(20) for category, counter in counters.items()}


def distribution_lines(series: pd.Series) -> list[str]:
    counts = series.value_counts(dropna=False)
    total = len(series)
    return [
        f"- `{label}`: {count} ({(count / total * 100 if total else 0):.1f}%)"
        for label, count in counts.items()
    ]


def flag_percentages(df: pd.DataFrame) -> dict[str, float]:
    return {
        column: float(df[column].mean() * 100) if len(df) else 0.0
        for column in FLAG_COLUMNS
    }


def comparison_to_5000(df: pd.DataFrame) -> list[str]:
    comparison_path = OUTPUTS_DIR / "youtube_sample_predictions_with_rules_v2_5000.csv"
    if not comparison_path.exists():
        comparison_path = OUTPUTS_DIR / "youtube_predictions_with_rules_v2_5000.csv"

    if not comparison_path.exists():
        return ["- No se encontro una salida v2 de 5000 para comparar."]

    base_df = pd.read_csv(comparison_path, encoding="utf-8-sig")
    current = flag_percentages(df)
    base = flag_percentages(base_df)

    lines = [f"- Comparacion contra `{comparison_path.name}`:"]
    for column in FLAG_COLUMNS:
        diff = current[column] - base[column]
        status = "estable" if abs(diff) <= 2 else ("sube" if diff > 0 else "baja")
        lines.append(
            f"- `{column}`: actual={current[column]:.1f}%, 5000={base[column]:.1f}%, diferencia={diff:+.1f} pp ({status})"
        )

    unusual = [
        column for column in FLAG_COLUMNS
        if current[column] > 40 and column != "has_political_mention"
    ]
    if unusual:
        lines.append(f"- Senales a revisar por posible disparo alto: {', '.join(f'`{col}`' for col in unusual)}.")
    else:
        lines.append("- No se observa una regla local disparada de forma excesiva fuera de lo esperable para esta muestra.")

    return lines


def top_risk_comments(df: pd.DataFrame, limit: int = 20) -> list[str]:
    columns = ["local_risk_score", "local_rule_tags", "pred_binary_label", "pred_multiclass_label", "message_raw"]
    top_df = df.sort_values("local_risk_score", ascending=False)[columns].head(limit)
    lines = []
    for row in top_df.to_dict(orient="records"):
        message = str(row["message_raw"]).replace("\n", " ").strip()
        if len(message) > 260:
            message = message[:257] + "..."
        lines.append(
            f"- score={row['local_risk_score']} tags=`{row['local_rule_tags']}` "
            f"binary=`{row['pred_binary_label']}` multiclass=`{row['pred_multiclass_label']}`: {message}"
        )
    return lines


def build_dashboard_aggregates(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    working_df = df.copy()
    group_columns = ["source_file", "video_id", "minute"]

    offset = pd.to_numeric(working_df.get("video_offset_msec"), errors="coerce")
    if offset.notna().any():
        working_df["minute"] = (offset.fillna(0) // 60000).astype(int)
        note = "Agregados por minuto generados con `video_offset_msec`."
    elif "timestamp_text" in working_df.columns:
        parsed = pd.to_timedelta(working_df["timestamp_text"], errors="coerce")
        if parsed.notna().any():
            working_df["minute"] = (parsed.dt.total_seconds().fillna(0) // 60).astype(int)
            note = "Agregados por minuto generados con `timestamp_text`."
        else:
            working_df["minute"] = "global"
            group_columns = ["minute"]
            note = "No se encontro timestamp usable; se genero agregado global."
    else:
        working_df["minute"] = "global"
        group_columns = ["minute"]
        note = "No se encontro timestamp usable; se genero agregado global."

    working_df["is_offensive_pred"] = working_df["pred_binary_label"].eq("ofensivo")
    grouped = (
        working_df.groupby(group_columns, dropna=False)
        .agg(
            comentarios=("message_raw", "size"),
            ofensivos=("is_offensive_pred", "sum"),
            terruqueo=("has_terruqueo", "sum"),
            fraude=("has_fraude", "sum"),
            polarizacion=("has_polarization_signal", "sum"),
            spam=("is_spam_noise", "sum"),
            avg_local_risk_score=("local_risk_score", "mean"),
        )
        .reset_index()
    )
    return grouped, note


def build_overview_report(df: pd.DataFrame, sample_size: int, aggregates_note: str) -> str:
    lines = [
        f"# YouTube {sample_size} Overview",
        "",
        f"Total procesado: **{len(df)}** comentarios de `youtube_lake.csv`.",
        "",
        aggregates_note,
        "",
        "## Distribucion pred_binary",
        "",
        *distribution_lines(df["pred_binary_label"]),
        "",
        "## Distribucion pred_multiclass",
        "",
        *distribution_lines(df["pred_multiclass_label"]),
        "",
        "## Conteo de flags locales",
        "",
    ]

    for column in FLAG_COLUMNS:
        count = int(df[column].sum())
        percentage = (count / len(df) * 100) if len(df) else 0
        lines.append(f"- `{column}`: {count} ({percentage:.1f}%)")

    lines.extend(
        [
            "",
            "## Local risk score",
            "",
            f"- promedio: {df['local_risk_score'].mean():.2f}",
            f"- minimo: {int(df['local_risk_score'].min())}",
            f"- maximo: {int(df['local_risk_score'].max())}",
        ]
    )

    lines.extend(["", "## Top 20 terminos activados por categoria", ""])
    for category, terms in top_terms(df).items():
        rendered = ", ".join(f"`{term}`={count}" for term, count in terms) if terms else "sin activaciones"
        lines.append(f"- `{category}`: {rendered}")

    example_specs = [
        ("Ejemplos de terruqueo", "has_terruqueo"),
        ("Ejemplos de discriminacion etnico-racial", "has_ethnic_racial_slur"),
        ("Ejemplos de fraude", "has_fraude"),
        ("Ejemplos de polarizacion", "has_polarization_signal"),
        ("Ejemplos de lenguaje homofobico", "has_homophobic_slur"),
        ("Ejemplos de insulto general", "has_general_insult"),
    ]
    for title, flag in example_specs:
        lines.extend(["", f"## {title}", ""])
        examples = examples_for(df, flag)
        if not examples:
            lines.append("- No se encontraron ejemplos en esta muestra.")
        else:
            lines.extend(f"- {example}" for example in examples)

    lines.extend(["", "## Top comentarios con mayor local_risk_score", ""])
    lines.extend(top_risk_comments(df))

    lines.extend(["", "## Comparacion breve contra muestra de 5000", ""])
    lines.extend(comparison_to_5000(df))

    lines.extend(
        [
            "",
            "## Advertencia de contexto",
            "",
            "Estas reglas son multilabel y no producen una etiqueta final unica. `onpe`, `jne` y `actas` activan instituciones electorales, pero no fraude por si solas. Terminos como `cholo`, `paisano`, `indio` o `serrano` activan una alerta etnico-racial porque pueden ser discriminatorios, aunque requieren revision contextual. La vulgaridad sola no debe interpretarse como odio, y polarizacion politica tampoco significa automaticamente discurso de odio.",
        ]
    )

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    ensure_directories()

    binary_model = joblib.load(ARTIFACTS_DIR / "binary_model.joblib")
    vectorizer_binary = joblib.load(ARTIFACTS_DIR / "vectorizer_binary.joblib")
    multiclass_model = joblib.load(ARTIFACTS_DIR / "multiclass_model.joblib")
    vectorizer_multiclass = joblib.load(ARTIFACTS_DIR / "vectorizer_multiclass.joblib")

    youtube_df = pd.read_csv(YOUTUBE_CSV_PATH, encoding="utf-8-sig")
    sample_size = min(args.sample_size, len(youtube_df))
    sampled_df = youtube_df.sample(n=sample_size, random_state=args.seed).reset_index(drop=True)
    sampled_df["message_raw"] = sampled_df["message"].fillna("").astype(str)
    sampled_df["message_clean"] = sampled_df["message_raw"].map(clean_text)

    X_binary = vectorizer_binary.transform(sampled_df["message_clean"])
    X_multiclass = vectorizer_multiclass.transform(sampled_df["message_clean"])

    pred_binary = binary_model.predict(X_binary)
    proba_binary = binary_model.predict_proba(X_binary)
    pred_multiclass = multiclass_model.predict(X_multiclass)
    proba_multiclass = multiclass_model.predict_proba(X_multiclass)

    sampled_df["pred_binary"] = pred_binary
    sampled_df["pred_binary_label"] = sampled_df["pred_binary"].map(BINARY_LABELS)
    sampled_df["confidence_binary"] = proba_binary.max(axis=1)
    sampled_df["pred_binary_confidence"] = sampled_df["confidence_binary"]
    sampled_df["pred_multiclass"] = pred_multiclass
    sampled_df["pred_multiclass_label"] = sampled_df["pred_multiclass"].map(MULTICLASS_MAP)
    sampled_df["confidence_multiclass"] = proba_multiclass.max(axis=1)
    sampled_df["pred_multiclass_confidence"] = sampled_df["confidence_multiclass"]

    base_output_columns = [
        "source_file",
        "video_id",
        "timestamp_text",
        "video_offset_msec",
        "author",
        "message_raw",
        "message_clean",
        "pred_binary",
        "pred_binary_label",
        "confidence_binary",
        "pred_binary_confidence",
        "pred_multiclass",
        "pred_multiclass_label",
        "confidence_multiclass",
        "pred_multiclass_confidence",
    ]
    base_output = sampled_df[[column for column in base_output_columns if column in sampled_df.columns]].copy()
    base_output_path = OUTPUTS_DIR / f"youtube_sample_predictions_{sample_size}.csv"
    base_output.to_csv(base_output_path, index=False, encoding="utf-8-sig")

    rules_df = pd.DataFrame(list(sampled_df["message_raw"].map(apply_rules)))
    enriched_output = pd.concat([base_output, rules_df], axis=1)
    enriched_output_path = OUTPUTS_DIR / f"youtube_predictions_with_rules_v2_{sample_size}.csv"
    enriched_output.to_csv(enriched_output_path, index=False, encoding="utf-8-sig")

    aggregates, aggregates_note = build_dashboard_aggregates(enriched_output)
    aggregates_path = OUTPUTS_DIR / f"youtube_dashboard_aggregates_v2_{sample_size}.csv"
    aggregates.to_csv(aggregates_path, index=False, encoding="utf-8-sig")

    report_path = REPORTS_DIR / f"youtube_{sample_size}_overview.md"
    report_path.write_text(build_overview_report(enriched_output, sample_size, aggregates_note), encoding="utf-8")

    print(f"CSV base generado en: {base_output_path}")
    print(f"CSV con reglas v2 generado en: {enriched_output_path}")
    print(f"Agregados dashboard generados en: {aggregates_path}")
    print(f"Reporte generado en: {report_path}")


if __name__ == "__main__":
    main()
