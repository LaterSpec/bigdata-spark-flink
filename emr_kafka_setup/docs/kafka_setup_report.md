# Kafka distribuido en EMR

> Actualización 2026-06-30: Kafka opera como quorum KRaft de tres nodos. El dashboard consulta deltas cada 3 segundos, inventario cada 5 segundos y muestra **conectado** con foco verde mientras la sesión AWS esté activa.

`EMR_PRIMARY` identifica un clúster EMR de tres instancias. Cada nodo ejecuta Kafka 3.6.2 con roles combinados `broker,controller`.

Kafka es la capa anterior a todo cómputo operativo: el producer publica `raw_youtube_chat` en este clúster y, desde allí, Flink y Spark consumen los mismos eventos de forma independiente en `EMR_WORKERS`.

| Propiedad | Valor |
|---|---|
| Brokers y controllers | 3 |
| Particiones por topic | 3 |
| Replication factor | 3 |
| Minimum ISR | 2 |
| Retención | 24 horas |

Los DNS privados se descubren en cada arranque. Los core nodes se administran mediante SSH con salto por el primary; la PEM permanece local.

## Topics

- `raw_youtube_chat`: eventos del data lake.
- `nlp_stream_results`: resultados Flink.
- `alerts_polarization`: alertas Flink.
- `nlp_batch_results`: reservado para resultados batch publicados.

## Arranque

```bash
cd emr_kafka_setup/dashboard
./scripts/bootstrap_emr_streaming.sh
```

El arranque estándar reinicializa KRaft. Usa `--no-reset-topics` únicamente cuando los tres brokers ya comparten la topología.

## Monitor

```bash
python3 /home/hadoop/bigdata-kafka/scripts/monitor_kafka_flow.py --once
python3 /home/hadoop/bigdata-kafka/scripts/monitor_kafka_flow.py --follow
```

El monitor consulta metadata y offsets sin consumir eventos. Registra quorum, ISR, rates, lag y errores recientes en snapshot JSON y JSONL rotativo.

## Criterios de salud

- Tres voters y un controller líder.
- Cero particiones offline.
- Cero particiones under-replicated.
- Broker local activo y topics accesibles.

Kafka es reconstruible desde S3 Raw. Reinicializar topics elimina mensajes y offsets Kafka, pero no elimina el CSV ni outputs S3.
