from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATASET_DIR = PROJECT_ROOT / "dataset"
YOUTUBE_CSV_PATH = PROJECT_ROOT / "youtube_lake.csv"
ARTIFACTS_DIR = BASE_DIR / "artifacts"
REPORTS_DIR = BASE_DIR / "reports"
OUTPUTS_DIR = BASE_DIR / "outputs"
DATA_CACHE_DIR = BASE_DIR / "data_cache"

TEXT_COLUMN = "comment"
YOUTUBE_TEXT_COLUMN = "message"

MULTICLASS_MAP = {
    0: "ofensivo_directo",
    1: "odio_agresion_grupal",
    2: "neutral_no_ofensivo",
    3: "vulgaridad_contextual",
}

BINARY_MAP = {
    0: 1,
    1: 1,
    2: 0,
    3: 0,
}

BINARY_NAME_MAP = {
    0: "no_ofensivo",
    1: "ofensivo",
}


def ensure_directories() -> None:
    for path in (ARTIFACTS_DIR, REPORTS_DIR, OUTPUTS_DIR, DATA_CACHE_DIR):
        path.mkdir(parents=True, exist_ok=True)


def clean_text(text: Any) -> str:
    if text is None:
        return ""

    text = str(text)
    text = text.replace("\u200b", " ").replace("\ufeff", " ")
    text = re.sub(r"https?://\S+|www\.\S+", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def load_split_dataframe(split_name: str) -> pl.DataFrame:
    path = DATASET_DIR / f"{split_name}.parquet"
    return pl.read_parquet(path)


def prepare_split_dataframe(split_name: str) -> pl.DataFrame:
    df = load_split_dataframe(split_name)
    prepared = df.with_columns(
        [
            pl.col(TEXT_COLUMN).cast(pl.Utf8).fill_null("").alias("text_raw"),
            pl.col(TEXT_COLUMN)
            .cast(pl.Utf8)
            .fill_null("")
            .map_elements(clean_text, return_dtype=pl.Utf8)
            .alias("text_clean"),
            pl.col("label").alias("label_original"),
            pl.col("label").replace_strict(BINARY_MAP).alias("label_binary_v2"),
            pl.col("label")
            .replace_strict(BINARY_MAP)
            .replace_strict(BINARY_NAME_MAP)
            .alias("label_binary_name_v2"),
            pl.col("label").replace_strict(MULTICLASS_MAP).alias("label_multiclass_name"),
        ]
    )
    return prepared


def save_cached_split(split_name: str, df: pl.DataFrame) -> Path:
    cache_path = DATA_CACHE_DIR / f"{split_name}_prepared_v2.csv"
    df.write_csv(cache_path)
    return cache_path


def label_distribution(df: pl.DataFrame, label_col: str = "label") -> list[dict[str, Any]]:
    return df.group_by(label_col).len().sort(label_col).to_dicts()


def write_json(path: Path, payload: Any) -> None:
    def default_serializer(value: Any) -> Any:
        if hasattr(value, "item"):
            return value.item()
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=default_serializer),
        encoding="utf-8",
    )


def write_markdown(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def confusion_matrix_records(matrix: list[list[int]], labels: list[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for actual_index, actual_label in enumerate(labels):
        row = {"actual_label": actual_label}
        for predicted_index, predicted_label in enumerate(labels):
            row[f"pred_{predicted_label}"] = int(matrix[actual_index][predicted_index])
        records.append(row)
    return records
