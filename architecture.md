flowchart LR

    %% FUENTE PRINCIPAL
    A["YouTube Live Chat<br/>JSON / CSV raw<br/>160,464 comentarios"] --> B["Data Lake Raw<br/>carpeta raw / S3"]

    %% CAMINO STREAMING
    B --> C["Python Producer<br/>lee CSV fila por fila"]
    C --> D["Kafka topic:<br/>raw_youtube_chat"]
    D --> E["Flink Streaming"]

    E --> F["Limpieza streaming"]
    E --> G["Conteo por ventanas"]
    E --> H["Detección de palabras políticas<br/>terruqueo / fraude / candidatos"]
    E --> I["Polarización por candidato"]
    E --> J["Alertas de toxicidad"]

    F --> K["Kafka topic:<br/>nlp_classified_chat"]
    G --> K
    H --> K
    I --> K
    J --> L["Kafka topic opcional:<br/>alerts_polarization"]

    K --> M["Dashboard"]
    L --> M

    %% CAMINO BATCH
    B --> N["Spark Batch"]

    N --> O["Limpieza histórica"]
    N --> P["Análisis exploratorio<br/>frecuencias / tendencias / spam"]
    N --> Q["Inferencia batch<br/>clasificar comentarios peruanos"]
    N --> R["Agregados históricos"]
    N --> S["Métricas ML<br/>accuracy / precision / recall / F1"]

    Q --> M
    R --> M
    S --> M

    %% DATASET EXTERNO
    T["Dataset externo etiquetado<br/>OffendES / Hate Speech / NewsCom-TOX"] --> U["Spark ML Training"]

    U --> V["Modelo base NLP<br/>odio / ofensividad / toxicidad"]
    V --> Q
    V --> E

    %% REGLAS LOCALES
    W["Reglas peruanas<br/>terruco, caviar, rojo,<br/>fraude, ONPE, JNE,<br/>Keiko, Fujimori, JP, FP"] --> E
    W --> Q