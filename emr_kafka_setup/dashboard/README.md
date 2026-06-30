# Radar Electoral Streaming Dashboard

Dashboard local para operar y observar la arquitectura distribuida:

```text
S3 Raw → EMR_PRIMARY [Producer → Kafka KRaft (3 brokers)]
                                  ├─→ EMR_WORKERS / Flink → Kafka results
                                  └─→ EMR_WORKERS / Spark → S3 Curated
```

Kafka es el punto de distribución: Flink y Spark consumen `raw_youtube_chat` de forma independiente.

## Inicio

```bash
./start_dashboard.sh
```

En PowerShell:

```powershell
.\start_dashboard.ps1
```

Abrir `http://127.0.0.1:8787` y pulsar **Conectar AWS**.

## Variables

El servidor lee `../../.env`:

```dotenv
EMR_PRIMARY=<cluster Kafka>
EMR_WORKERS=<cluster compute 1>,<cluster compute 2>
DATA_SIZE=30000
SPARK_BATCH_SIZE=1000
SPARK_MAX_CONCURRENCY=1
```

## API

| Endpoint | Función |
|---|---|
| `POST /api/aws/start` | Acepta el arranque asíncrono; después de un stop confirmado crea una sesión limpia. |
| `GET /api/aws/start/status` | Progreso y modo `fresh`, `recovery` o `resume` del arranque. |
| `POST /api/aws/stop` | Cancela y verifica toda la ejecución en `EMR_PRIMARY` y `EMR_WORKERS`; limpia salud y estados. |
| `GET /api/aws/status` | Offsets raw, `normalized_comments`, total de salidas Flink y threshold Spark elegible. |
| `GET /api/live-delta` | Eventos incrementales y normalizados entregados por topic y partición. |
| `POST /api/spark/start` | Agenda un rango Spark sin bloquear la petición; respeta orden y concurrencia. |
| `GET /api/spark/status` | Agrega estados por batch desde todos los workers. |
| `GET /api/spark/comments` | Extrae comentarios ofensivos del worker propietario. |
| `GET /api/pipeline/health` | Salud Kafka, Flink, YARN, Spark y compute. |

## Batches disjuntos

Cada múltiplo de `SPARK_BATCH_SIZE` crea un rango exclusivo:

```text
batch_0001000 → row_number 1–1000
batch_0002000 → row_number 1001–2000
```

El scheduler lanza como máximo `SPARK_MAX_CONCURRENCY` batches a la vez. El valor seguro por defecto es `1`; no inicia un rango posterior hasta que el anterior termina correctamente. Los workers se eligen round-robin y el launcher remoto vuelve a aplicar el límite para protegerse de pestañas duplicadas.

Cada `target` debe ser múltiplo de `SPARK_BATCH_SIZE`. El primer job valida que el rango produzca exactamente ese número de filas antes de ejecutar reglas, OffendES y scoring.

Al conectar o recargar, el frontend agrega primero los estados Spark de todos los workers y solo después calcula thresholds faltantes; así no relanza batches existentes por una carrera de polling.

El launcher es idempotente por defecto. Un reproceso manual de un batch terminado o fallido requiere `--force`; un lock activo nunca se sobrescribe.

## Panel de salud

El bloque **Salud de la plataforma** aparece al final y muestra:

- Estado de conexión y timestamp. Una sesión activa se presenta como **conectado** con foco verde.
- Tres brokers, controller líder, ISR y errores.
- Eventos y tasa por topic.
- Lag de grupos Flink.
- Nodos YARN y aplicaciones por compute.
- Jobs Flink y batches Spark.
- Acciones de recuperación cuando el diagnóstico técnico detecta degradación.

Cadencias:

| Dato | Frecuencia objetivo | Fuente |
|---|---:|---|
| Chat RAW y eventos Flink | 3 s | `/api/live-delta` |
| **Flink normalizados** | 3 s | offsets confirmados de `flink-job1-normalize` |
| Estados Spark | 3 s | `/api/spark/status` |
| Inventario Kafka y thresholds | 5 s | `/api/aws/status` |
| Salud integral | 5 s | `/api/pipeline/health` |

Las consultas no se solapan: si SSH tarda más que el intervalo, se espera a que termine la consulta en curso.

## Scripts

```bash
./scripts/bootstrap_emr_streaming.sh --limit 2000
./scripts/pipeline_health_from_aws.sh
./scripts/spark_status_from_aws.sh
./scripts/stop_emr_streaming.sh
```

El dashboard no usa snapshots falsos como fallback. Cuando AWS no responde, conserva el último estado visible y marca la plataforma como degradada u offline.

Después de reiniciar una sesión local:

```powershell
.\start_dashboard.ps1
.\scripts\restart_services_after_session.ps1
```

El segundo comando conserva topics y offsets; no debe usarse para solicitar una sesión limpia.

Si Kafka ya está activo pero el producer quedó detenido antes de `DATA_SIZE`, `POST /api/aws/start` usa una reanudación ligera: no redespliega brokers ni espera a `EMR_WORKERS`; continúa desde la siguiente fila pendiente. El diagnóstico de compute se mantiene visible como degradado si el worker no responde.

## Detención total

**Detener plataforma** ejecuta una parada distribuida explícita. La acción:

1. Bloquea nuevos batches y espera a los launchers Spark en curso.
2. Cancela aplicaciones YARN, drivers Spark y los cinco procesos Flink en todos los `EMR_WORKERS`.
3. Detiene producer, monitor y los tres brokers de `EMR_PRIMARY`.
4. Elimina snapshots de salud y estados locales de batches.
5. Limpia el dashboard y vuelve a habilitar **Conectar AWS** solo si ambos lados confirmaron la parada.
