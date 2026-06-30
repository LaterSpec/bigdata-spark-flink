# Arquitectura actual

Este diagrama es el resumen canónico. Kafka en `EMR_PRIMARY` recibe primero los eventos y los expone simultáneamente a Flink y Spark en la capa de cómputo.

```mermaid
flowchart LR
    L["S3 Data Lake Raw"]

    subgraph K["EMR_PRIMARY · Kafka primero"]
        P["Producer Lake→Kafka"]
        BUS["Kafka KRaft<br/>bus lógico"]
        RAW[("raw_youtube_chat")]
        OUT[("nlp_stream_results<br/>alerts_polarization")]
        B1["Broker/controller 1"]
        B2["Broker/controller 2"]
        B3["Broker/controller 3"]
        M["Monitor Kafka"]

        P --> BUS --> RAW
        BUS --- OUT
        BUS --- B1
        BUS --- B2
        BUS --- B3
        B1 <--> B2
        B2 <--> B3
        B3 <--> B1
        M -. observa .-> BUS
    end

    subgraph C["EMR_WORKERS · consumidores"]
        F["Flink<br/>5 jobs streaming"]
        SQ["Cola Spark<br/>concurrencia segura = 1"]
        S1["Batch activo<br/>rango disjunto"]
    end

    L --> P
    RAW --> F
    RAW --> SQ --> S1
    F --> OUT
    S1 --> Q["S3 Curated"]
    M --> D["Dashboard + salud"]
    OUT --> D
    Q --> D
```

Flink y Spark no forman una cadena entre sí. Son consumidores independientes del mismo topic raw: Flink produce resultados nuevamente en Kafka y Spark procesa batches disjuntos en orden seguro y persiste datasets en S3.

La especificación completa está en `architecture.md` en la raíz del proyecto.
