from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score, precision_score, recall_score

from utils import (
    ARTIFACTS_DIR,
    BINARY_NAME_MAP,
    DATA_CACHE_DIR,
    MULTICLASS_MAP,
    REPORTS_DIR,
    confusion_matrix_records,
    ensure_directories,
    label_distribution,
    prepare_split_dataframe,
    save_cached_split,
    write_json,
    write_markdown,
)

RANDOM_STATE = 42


def evaluate_predictions(y_true, y_pred, labels, target_names):
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision_macro": float(precision_score(y_true, y_pred, average="macro", zero_division=0)),
        "recall_macro": float(recall_score(y_true, y_pred, average="macro", zero_division=0)),
        "f1_macro": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "classification_report": classification_report(
            y_true,
            y_pred,
            labels=labels,
            target_names=target_names,
            zero_division=0,
            output_dict=True,
        ),
        "classification_report_text": classification_report(
            y_true,
            y_pred,
            labels=labels,
            target_names=target_names,
            zero_division=0,
        ),
        "confusion_matrix": confusion_matrix(y_true, y_pred, labels=labels).tolist(),
    }


def build_vectorizer() -> TfidfVectorizer:
    return TfidfVectorizer(
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.98,
        max_features=50000,
        strip_accents="unicode",
        sublinear_tf=True,
    )


def train_binary(train_df: pd.DataFrame, test_df: pd.DataFrame) -> dict:
    vectorizer = build_vectorizer()
    X_train = vectorizer.fit_transform(train_df["text_clean"])
    X_test = vectorizer.transform(test_df["text_clean"])

    model = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=RANDOM_STATE,
    )
    model.fit(X_train, train_df["label_binary_v2"])

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)
    metrics = evaluate_predictions(
        test_df["label_binary_v2"],
        y_pred,
        labels=[0, 1],
        target_names=["no_ofensivo", "ofensivo"],
    )

    joblib.dump(model, ARTIFACTS_DIR / "binary_model.joblib")
    joblib.dump(vectorizer, ARTIFACTS_DIR / "vectorizer_binary.joblib")

    return {
        "model": model,
        "vectorizer": vectorizer,
        "metrics": metrics,
        "probabilities": y_proba,
    }


def train_multiclass(train_df: pd.DataFrame, test_df: pd.DataFrame) -> dict:
    vectorizer = build_vectorizer()
    X_train = vectorizer.fit_transform(train_df["text_clean"])
    X_test = vectorizer.transform(test_df["text_clean"])

    model = LogisticRegression(
        max_iter=1500,
        class_weight="balanced",
        random_state=RANDOM_STATE,
    )
    model.fit(X_train, train_df["label_original"])

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)
    metrics = evaluate_predictions(
        test_df["label_original"],
        y_pred,
        labels=[0, 1, 2, 3],
        target_names=[MULTICLASS_MAP[idx] for idx in [0, 1, 2, 3]],
    )

    joblib.dump(model, ARTIFACTS_DIR / "multiclass_model.joblib")
    joblib.dump(vectorizer, ARTIFACTS_DIR / "vectorizer_multiclass.joblib")

    return {
        "model": model,
        "vectorizer": vectorizer,
        "metrics": metrics,
        "probabilities": y_proba,
    }


def save_confusion_csv(name: str, matrix: list[list[int]], labels: list[str]) -> None:
    df = pd.DataFrame(confusion_matrix_records(matrix, labels))
    df.to_csv(ARTIFACTS_DIR / f"{name}_confusion_matrix.csv", index=False, encoding="utf-8-sig")


def build_report(distributions: dict, binary_metrics: dict, multiclass_metrics: dict) -> str:
    binary_f1 = binary_metrics["f1_macro"]
    multiclass_f1 = multiclass_metrics["f1_macro"]
    recommended = (
        "El modelo binario debe ser la capa principal inicial del pipeline para detectar ofensividad general, "
        "mientras que el multiclase debe usarse como señal analitica complementaria para distinguir insulto directo, "
        "odio/agresion severa, neutralidad y vulgaridad contextual."
    )

    return f"""# Model Baseline Report

## Resumen

Se entreno un baseline local con OffendES para cubrir la parte de NLP supervisado del proyecto antes de integrarlo a Spark batch y Flink streaming. El objetivo de este baseline es validar una primera capa de clasificacion de ofensividad y agresion general en espanol usando los splits locales de `dataset/train.parquet`, `dataset/validation.parquet` y `dataset/test.parquet`.

## Por que usamos OffendES

OffendES aporta textos en espanol ya etiquetados para ofensividad, agresion y lenguaje problemático. Eso cubre una necesidad central del proyecto: el chat electoral peruano de `youtube_lake.csv` es valioso como fuente real, pero no viene anotado para entrenamiento supervisado. OffendES permite construir un modelo base defendible para el documento final, alineado con la parte de tecnicas NLP y clasificacion inicial de lenguaje ofensivo/odio/agresion general.

## Que parte del objetivo del documento cubre

Este baseline cubre la parte de entrenamiento y evaluacion de un modelo NLP base para deteccion automatica de:

- ofensividad general
- agresion verbal
- odio o agresion severa
- vulgaridad contextual

Eso se conecta directamente con el bloque de Spark ML Training y con la futura inferencia batch sobre comentarios peruanos. Tambien deja listo un artefacto inicial que puede convertirse despues en una etapa de inferencia para Flink.

## Limites del baseline

OffendES no cubre por si solo el contexto politico peruano. No reconoce de manera especializada categorias como:

- terruqueo
- narrativas de fraude electoral
- polarizacion entre candidatos o partidos peruanos
- spam/ruido caracteristico del live chat

Por eso se agrego una capa de reglas locales peruanas. El modelo aporta ofensividad general en espanol y las reglas complementan el contexto electoral peruano.

## Distribucion de labels

- Train: {json.dumps(distributions["train"], ensure_ascii=False)}
- Validation: {json.dumps(distributions["validation"], ensure_ascii=False)}
- Test: {json.dumps(distributions["test"], ensure_ascii=False)}

La clase `1 = odio_agresion_grupal` esta claramente desbalanceada. Por eso la metrica principal de comparacion es `F1 macro`, que penaliza mejor el mal rendimiento en clases minoritarias.

## Comparacion de modelos

### Modelo binario

- Definicion: `0/1 = ofensivo`, `2/3 = no_ofensivo`
- Accuracy: {binary_metrics["accuracy"]:.4f}
- Precision macro: {binary_metrics["precision_macro"]:.4f}
- Recall macro: {binary_metrics["recall_macro"]:.4f}
- F1 macro: {binary_f1:.4f}

### Modelo multiclase

- Clases: `ofensivo_directo`, `odio_agresion_grupal`, `neutral_no_ofensivo`, `vulgaridad_contextual`
- Accuracy: {multiclass_metrics["accuracy"]:.4f}
- Precision macro: {multiclass_metrics["precision_macro"]:.4f}
- Recall macro: {multiclass_metrics["recall_macro"]:.4f}
- F1 macro: {multiclass_f1:.4f}

## Recomendacion final

{recommended}

En terminos de pipeline:

- usar el binario como detector robusto y simple de ofensividad general
- usar el multiclase como una vista mas rica para analisis y dashboard
- usar reglas locales peruanas para `terruqueo`, `fraude`, `polarizacion politica` y `spam_ruido`

## Siguiente paso hacia la arquitectura Big Data

- Spark batch: usar este baseline para clasificar historicamente el `youtube_lake.csv`
- Flink streaming: aplicar el modelo como capa de scoring y combinarlo con reglas locales en tiempo real
- Dashboard: mostrar porcentaje ofensivo, clases detectadas y flags politicos por ventana
"""


def main() -> None:
    ensure_directories()

    train_pl = prepare_split_dataframe("train")
    validation_pl = prepare_split_dataframe("validation")
    test_pl = prepare_split_dataframe("test")

    for split_name, split_df in [("train", train_pl), ("validation", validation_pl), ("test", test_pl)]:
        save_cached_split(split_name, split_df)

    train_df = pd.DataFrame(train_pl.to_dicts())
    validation_df = pd.DataFrame(validation_pl.to_dicts())
    test_df = pd.DataFrame(test_pl.to_dicts())

    distributions = {
        "train": label_distribution(train_pl),
        "validation": label_distribution(validation_pl),
        "test": label_distribution(test_pl),
    }

    binary_result = train_binary(train_df, test_df)
    multiclass_result = train_multiclass(train_df, test_df)

    label_mapping = {
        "binary": {
            "0": "no_ofensivo",
            "1": "ofensivo",
            "source_labels": {"0": 1, "1": 1, "2": 0, "3": 0},
        },
        "multiclass": {str(key): value for key, value in MULTICLASS_MAP.items()},
    }
    write_json(ARTIFACTS_DIR / "label_mapping.json", label_mapping)

    metrics_summary = {
        "distributions": distributions,
        "binary": binary_result["metrics"],
        "multiclass": multiclass_result["metrics"],
        "validation_rows": len(validation_df),
        "data_cache_dir": str(DATA_CACHE_DIR),
    }
    write_json(ARTIFACTS_DIR / "metrics_summary.json", metrics_summary)

    save_confusion_csv(
        "binary",
        binary_result["metrics"]["confusion_matrix"],
        ["no_ofensivo", "ofensivo"],
    )
    save_confusion_csv(
        "multiclass",
        multiclass_result["metrics"]["confusion_matrix"],
        [MULTICLASS_MAP[idx] for idx in [0, 1, 2, 3]],
    )

    report_content = build_report(distributions, binary_result["metrics"], multiclass_result["metrics"])
    write_markdown(REPORTS_DIR / "model_baseline_report.md", report_content)

    print("=== Binary model ===")
    print(binary_result["metrics"]["classification_report_text"])
    print("=== Multiclass model ===")
    print(multiclass_result["metrics"]["classification_report_text"])
    print(f"Reporte generado en: {REPORTS_DIR / 'model_baseline_report.md'}")


if __name__ == "__main__":
    main()
