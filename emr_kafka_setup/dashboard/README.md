# Radar Electoral Streaming Dashboard

Dashboard local liviano para visualizar la arquitectura:

```text
S3 Raw -> Python Producer -> Kafka -> Spark Batch / Flink Streaming -> S3 Curated / Kafka Results -> Dashboard
```

## Abrir localmente

Desde `emr_kafka_setup/dashboard`:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_dashboard.ps1
```

Luego abrir:

```text
http://localhost:8787
```

El arranque usa `node server.js` si Node esta disponible. No requiere instalar paquetes.

## Refrescar datos desde AWS EMR

El dashboard no guarda claves ni credenciales en el navegador. La conexion funcional es local:

```text
Browser -> Node local /api/sync -> PowerShell -> SSH/SCP con final.pem -> EMR/Kafka -> data/*.jsonl
```

Desde la UI puedes pulsar `Sincronizar AWS`. Si el master EMR acepta SSH, el servidor local ejecuta `scripts/sync_from_aws.ps1`, trae muestras desde Kafka y la UI recarga los datos. Si AWS Academy rechaza temporalmente SSH, se muestra un error controlado y se mantiene el snapshot local validado.

Tambien puedes ejecutarlo manualmente:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync_from_aws.ps1
```

Archivos actualizados:

- `data/flink_nlp_stream_results_sample.jsonl`
- `data/flink_alerts_sample.jsonl`
- `data/dashboard_counts.json`

## Que muestra

- Cinta viva de mensajes y eventos de streaming.
- Alertas desde `alerts_polarization`.
- Senales Flink de normalizacion, reglas, ventanas y polarizacion.
- Estado Spark Batch desde Kafka con outputs S3.
- Grafico simple de volumen/alertas y barras por actor politico.
- Estado de conexion local/AWS.

## Nota

La UI incluye `data/dashboard_snapshot.json` como snapshot local enriquecido para operar offline. Los archivos `data/flink_nlp_stream_results_sample.jsonl`, `data/flink_alerts_sample.jsonl` y `data/dashboard_counts.json` se reemplazan cuando se sincroniza contra AWS.
