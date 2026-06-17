# AWS/S3/EMR Setup - Proyecto Big Data

Este archivo documenta la preparacion de S3 y EMR para la fase Spark Batch del proyecto de deteccion de discurso ofensivo, discriminatorio, terruqueo y polarizacion politica en comentarios de YouTube Live Chat electoral peruano.

La fase documentada aqui ya queda como base operativa actual. La nueva arquitectura objetivo del proyecto agrega una capa streaming en AWS con Kafka y Flink, pero esa expansion todavia no se implementa en este documento.

## Contexto local

- Workspace local: `C:\Users\itsma\Documents\BigData\Proyectofinal`
- Bucket S3: `s3://figuretibucket/`
- EMR master public DNS: `ec2-100-56-102-9.compute-1.amazonaws.com`
- Usuario validado para EMR: `hadoop`
- Llave SSH local: `final.pem`

Nota: en esta maquina Windows, `aws` no estaba disponible en PATH, por eso se uso el flujo alternativo local -> SCP -> EMR -> S3.

## Validaciones realizadas

Validacion local de rutas:

```powershell
Get-Item youtube_lake.csv
Get-Item dataset/train.parquet
Get-Item dataset/validation.parquet
Get-Item dataset/test.parquet
Get-Item local_baseline
```

Rutas solicitadas que no existian exactamente:

```text
dataset/prepared/train.parquet
dataset/prepared/validation.parquet
test.parquet
```

Rutas usadas para OffendES:

```text
dataset/train.parquet
dataset/validation.parquet
dataset/test.parquet
```

Validacion de AWS CLI local:

```powershell
aws --version
```

Resultado: `aws` no estaba instalado o no estaba en PATH local.

Validacion de acceso EMR + S3:

```powershell
ssh -i .\final.pem -o BatchMode=yes -o StrictHostKeyChecking=no hadoop@ec2-100-56-102-9.compute-1.amazonaws.com "hostname; whoami; aws --version; aws s3 ls s3://figuretibucket/"
```

Resultado confirmado:

```text
whoami: hadoop
aws-cli/1.18.147
s3://figuretibucket/ accesible desde EMR
```

## Permisos de la llave PEM en Windows

OpenSSH rechazo inicialmente `final.pem` por permisos demasiado abiertos. Se corrigio con:

```powershell
icacls .\final.pem /inheritance:r /remove:g "LaterSpec\CodexSandboxUsers" /grant:r "LATERSPEC\itsma:R"
```

## Comandos usados para staging en EMR

Crear staging remoto:

```powershell
ssh -i .\final.pem -o BatchMode=yes -o StrictHostKeyChecking=no hadoop@ec2-100-56-102-9.compute-1.amazonaws.com "rm -rf /home/hadoop/proyectofinal_s3_upload && mkdir -p /home/hadoop/proyectofinal_s3_upload/dataset /home/hadoop/proyectofinal_s3_upload/baseline"
```

Empaquetar `local_baseline` sin `.venv` ni caches:

```powershell
tar -czf local_baseline_s3_upload.tar.gz --exclude='local_baseline/.venv' --exclude='local_baseline/__pycache__' --exclude='local_baseline/*/__pycache__' local_baseline
```

Copiar archivos al master EMR:

```powershell
scp -i .\final.pem -o BatchMode=yes -o StrictHostKeyChecking=no .\youtube_lake.csv hadoop@ec2-100-56-102-9.compute-1.amazonaws.com:/home/hadoop/proyectofinal_s3_upload/youtube_lake.csv
scp -i .\final.pem -o BatchMode=yes -o StrictHostKeyChecking=no .\dataset\train.parquet .\dataset\validation.parquet .\dataset\test.parquet hadoop@ec2-100-56-102-9.compute-1.amazonaws.com:/home/hadoop/proyectofinal_s3_upload/dataset/
scp -i .\final.pem -o BatchMode=yes -o StrictHostKeyChecking=no .\local_baseline_s3_upload.tar.gz hadoop@ec2-100-56-102-9.compute-1.amazonaws.com:/home/hadoop/proyectofinal_s3_upload/baseline/
```

## Comandos usados desde EMR hacia S3

Subida de datos principales:

```bash
aws s3 cp /home/hadoop/proyectofinal_s3_upload/youtube_lake.csv s3://figuretibucket/data/raw/youtube/youtube_lake.csv
aws s3 cp /home/hadoop/proyectofinal_s3_upload/dataset/train.parquet s3://figuretibucket/dataset/offendES/train.parquet
aws s3 cp /home/hadoop/proyectofinal_s3_upload/dataset/validation.parquet s3://figuretibucket/dataset/offendES/validation.parquet
aws s3 cp /home/hadoop/proyectofinal_s3_upload/dataset/test.parquet s3://figuretibucket/dataset/offendES/test.parquet
```

Subida del baseline:

```bash
cd /home/hadoop/proyectofinal_s3_upload/baseline
mkdir -p extracted
tar -xzf local_baseline_s3_upload.tar.gz -C extracted
aws s3 sync extracted/local_baseline/ s3://figuretibucket/codes/local_baseline/
```

Se crearon placeholders `.keep` para prefixes vacios de batch, streaming, logs y carpetas de codigo futuras.

## Estructura final esperada en S3

```text
s3://figuretibucket/
  codes/
    local_baseline/
      artifacts/
      data_cache/
      outputs/
      reports/
      README.md
      peruvian_rules.py
      requirements.txt
      run_inference_youtube.py
      test_peruvian_rules.py
      train_baseline.py
      utils.py
    spark/
      .keep
    flink/
      .keep
    producer/
      .keep

  data/
    raw/
      youtube/
        youtube_lake.csv
    processed/
      youtube_classified/
        .keep

  dataset/
    offendES/
      train.parquet
      validation.parquet
      test.parquet

  output/
    batch/
      predictions/
        .keep
      aggregates_by_minute/
        .keep
      reports/
        .keep
    streaming/
      classified/
        .keep
      alerts/
        .keep
      metrics/
        .keep

  logs/
    emr/
      .keep
```

## Verificacion realizada en S3 desde EMR

```bash
aws s3 ls s3://figuretibucket/
aws s3 ls s3://figuretibucket/data/raw/youtube/
aws s3 ls s3://figuretibucket/dataset/offendES/
aws s3 ls s3://figuretibucket/codes/local_baseline/
aws s3 ls s3://figuretibucket/codes/local_baseline/artifacts/
aws s3 ls s3://figuretibucket/output/batch/
aws s3 ls s3://figuretibucket/output/streaming/
aws s3 ls s3://figuretibucket/logs/emr/
```

Archivos clave confirmados:

```text
s3://figuretibucket/data/raw/youtube/youtube_lake.csv
s3://figuretibucket/dataset/offendES/train.parquet
s3://figuretibucket/dataset/offendES/validation.parquet
s3://figuretibucket/dataset/offendES/test.parquet
s3://figuretibucket/codes/local_baseline/artifacts/binary_model.joblib
s3://figuretibucket/codes/local_baseline/artifacts/multiclass_model.joblib
s3://figuretibucket/codes/local_baseline/artifacts/vectorizer_binary.joblib
s3://figuretibucket/codes/local_baseline/artifacts/vectorizer_multiclass.joblib
```

Conteo confirmado para `codes/local_baseline/`:

```text
33 objetos
```

## Comandos alternativos recomendados

Si AWS CLI se instala localmente en Windows, se puede subir directamente:

```powershell
aws s3 cp .\youtube_lake.csv s3://figuretibucket/data/raw/youtube/youtube_lake.csv
aws s3 cp .\dataset\train.parquet s3://figuretibucket/dataset/offendES/train.parquet
aws s3 cp .\dataset\validation.parquet s3://figuretibucket/dataset/offendES/validation.parquet
aws s3 cp .\dataset\test.parquet s3://figuretibucket/dataset/offendES/test.parquet
aws s3 sync .\local_baseline\ s3://figuretibucket/codes/local_baseline/ --exclude ".venv/*" --exclude "__pycache__/*"
```

Si se mantiene el flujo por EMR:

```powershell
scp -i .\final.pem .\archivo_local hadoop@ec2-100-56-102-9.compute-1.amazonaws.com:/home/hadoop/
ssh -i .\final.pem hadoop@ec2-100-56-102-9.compute-1.amazonaws.com
aws s3 cp /home/hadoop/archivo_local s3://figuretibucket/ruta/destino/
```

## Como conectarse al EMR

```powershell
ssh -i .\final.pem hadoop@ec2-100-56-102-9.compute-1.amazonaws.com
```

Verificar S3 desde EMR:

```bash
aws s3 ls s3://figuretibucket/
aws s3 ls s3://figuretibucket/data/raw/youtube/
aws s3 ls s3://figuretibucket/dataset/offendES/
```

## Advertencia de costos

Apagar o terminar el cluster EMR cuando no se este usando. Mantener 1 master + 2 workers encendidos puede generar costos mientras el cluster siga vivo, incluso si no hay jobs Spark corriendo.

## Siguiente paso

El siguiente paso ya no es Spark Batch, porque esa fase quedo completada. A partir de esta base, la continuacion recomendada en AWS es:

1. Definir la topologia de Kafka en AWS.
2. Elegir el servicio de Kafka administrado o autogestionado.
3. Definir topics, particiones, retencion y seguridad.
4. Diseñar la integracion futura entre producer, Kafka, Flink, S3 y dashboard.

Referencia principal para esa nueva etapa: `architecture.md`.
