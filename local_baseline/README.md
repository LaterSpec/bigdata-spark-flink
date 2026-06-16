# Local Baseline NLP

Baseline local para entrenar y evaluar un clasificador NLP con OffendES y luego aplicarlo a una muestra de `youtube_lake.csv` junto con reglas locales peruanas.

## Estructura

- `train_baseline.py`: entrena modelos binario y multiclase, evalua y genera artefactos.
- `run_inference_youtube.py`: carga modelos entrenados y produce CSVs de prediccion sobre YouTube.
- `peruvian_rules.py`: reglas lexico-contextuales para terruqueo, fraude, polarizacion y spam/ruido.
- `utils.py`: rutas, limpieza de texto, carga de datos y utilidades comunes.
- `artifacts/`: modelos serializados y resumen de metricas.
- `reports/`: reporte markdown del baseline.
- `outputs/`: CSVs de inferencia.
- `data_cache/`: splits derivados con labels binarios corregidos.

## Uso

Desde la raiz del proyecto:

```powershell
python -m venv .\local_baseline\.venv
.\local_baseline\.venv\Scripts\python -m pip install --upgrade pip
.\local_baseline\.venv\Scripts\python -m pip install -r .\local_baseline\requirements.txt
.\local_baseline\.venv\Scripts\python .\local_baseline\train_baseline.py
.\local_baseline\.venv\Scripts\python .\local_baseline\run_inference_youtube.py --sample-size 500
```

## Labels

Binario:

- `1 = ofensivo` para labels originales `0` y `1`
- `0 = no_ofensivo` para labels originales `2` y `3`

Multiclase:

- `0 = ofensivo_directo`
- `1 = odio_agresion_grupal`
- `2 = neutral_no_ofensivo`
- `3 = vulgaridad_contextual`
