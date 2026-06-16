from __future__ import annotations

from pathlib import Path

import polars as pl

DATASET_DIR = Path("dataset")
OUTPUT_DIR = DATASET_DIR / "prepared"

MULTICLASS_MAP = {
    0: "ofensivo_directo",
    1: "odio_agresion_grupal",
    2: "neutral_no_ofensivo",
    3: "vulgaridad_contextual",
}


def enrich_labels(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col("label").replace_strict(MULTICLASS_MAP).alias("label_name"),
            pl.when(pl.col("label") == 2)
            .then(0)
            .otherwise(1)
            .alias("label_binary"),
            pl.when(pl.col("label") == 2)
            .then(pl.lit("no_ofensivo"))
            .otherwise(pl.lit("ofensivo_o_toxico"))
            .alias("label_binary_name"),
        ]
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for split in ("train", "validation", "test"):
        source = DATASET_DIR / f"{split}.parquet"
        target_parquet = OUTPUT_DIR / f"{split}_labeled.parquet"
        target_csv = OUTPUT_DIR / f"{split}_labeled.csv"

        df = pl.read_parquet(source)
        enriched = enrich_labels(df)

        enriched.write_parquet(target_parquet)
        enriched.write_csv(target_csv)

        print(f"{split}: {enriched.height} filas -> {target_parquet}")


if __name__ == "__main__":
    main()
