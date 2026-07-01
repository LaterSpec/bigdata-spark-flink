# Informe final LaTeX

**Repositorio del proyecto:** [github.com/LaterSpec/bigdata-spark-flink](https://github.com/LaterSpec/bigdata-spark-flink)

Artefactos:

- `informe_final.tex`: código fuente del informe.
- `informe_final.pdf`: versión compilada.

Compilación con Tectonic:

```powershell
tectonic informe_final.tex --keep-logs --keep-intermediates
```

El informe documenta la arquitectura vigente. Los valores de métricas corresponden a las validaciones registradas en `docs/DISTRIBUTED_RUNTIME_VALIDATION.md`, `docs/SPARK_BATCH_RECORDS.md` y los reportes de baseline; no son una medición inventada en tiempo real.
