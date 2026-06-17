param(
  [string]$PemPath = "..\..\final.pem",
  [string]$HostName = "",
  [string]$User = "hadoop",
  [int]$NlpMessages = 7000,
  [int]$AlertMessages = 500
)

$ErrorActionPreference = "Stop"

$DashboardRoot = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $DashboardRoot "data"
New-Item -ItemType Directory -Force $DataDir | Out-Null

if ([string]::IsNullOrWhiteSpace($HostName)) {
  $EnvPath = Join-Path $DashboardRoot "..\..\.env"
  if (Test-Path $EnvPath) {
    $EnvPrimary = Get-Content $EnvPath |
      Where-Object { $_ -match "^\s*EMR_PRIMARY\s*=" } |
      Select-Object -First 1
    if ($EnvPrimary) {
      $HostName = ($EnvPrimary -replace "^\s*EMR_PRIMARY\s*=\s*", "").Trim().Trim('"').Trim("'")
    }
  }
}

if ([string]::IsNullOrWhiteSpace($HostName)) {
  throw "No se encontro HostName. Define EMR_PRIMARY en .env o usa -HostName."
}

$ResolvedPemPath = if ([System.IO.Path]::IsPathRooted($PemPath)) {
  $PemPath
} else {
  Join-Path $DashboardRoot $PemPath
}

if (-not (Test-Path $ResolvedPemPath)) {
  throw "No se encontro el archivo PEM en $ResolvedPemPath"
}

$sshTarget = "$User@$HostName"
$remoteScript = @'
set -e
RAW=$(/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic raw_youtube_chat | awk -F: '{sum += $3} END {print sum+0}')
NLP=$(/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic nlp_stream_results | awk -F: '{sum += $3} END {print sum+0}')
ALERTS=$(/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic alerts_polarization | awk -F: '{sum += $3} END {print sum+0}')
timeout 180s /home/hadoop/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nlp_stream_results --from-beginning --max-messages __NLP_MESSAGES__ > /home/hadoop/bigdata-kafka/logs/dashboard_nlp_stream_results_sample.jsonl || true
timeout 60s /home/hadoop/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts_polarization --from-beginning --max-messages __ALERT_MESSAGES__ > /home/hadoop/bigdata-kafka/logs/dashboard_alerts_sample.jsonl || true
aws s3 cp s3://figuretibucket/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/summary.md /home/hadoop/bigdata-kafka/logs/spark_job2_rules_summary.md >/dev/null
aws s3 cp s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/summary.md /home/hadoop/bigdata-kafka/logs/spark_job4_ml_summary.md >/dev/null
aws s3 cp s3://figuretibucket/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/summary.md /home/hadoop/bigdata-kafka/logs/spark_job5_hybrid_summary.md >/dev/null
printf '{"generated_at":"%s","data_mode":"aws_synced","counts":{"raw_youtube_chat":%s,"nlp_stream_results":%s,"alerts_polarization":%s,"spark_curated_rows":105,"spark_aggregates_rows":54}}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RAW" "$NLP" "$ALERTS" > /home/hadoop/bigdata-kafka/logs/dashboard_counts.json
'@

$remoteScript = $remoteScript.Replace("__NLP_MESSAGES__", [string]$NlpMessages).Replace("__ALERT_MESSAGES__", [string]$AlertMessages)

ssh -i $ResolvedPemPath -o StrictHostKeyChecking=no $sshTarget $remoteScript
if ($LASTEXITCODE -ne 0) { throw "Fallo SSH contra $sshTarget" }

scp -i $ResolvedPemPath -o StrictHostKeyChecking=no "${sshTarget}:/home/hadoop/bigdata-kafka/logs/dashboard_nlp_stream_results_sample.jsonl" (Join-Path $DataDir "flink_nlp_stream_results_sample.jsonl")
if ($LASTEXITCODE -ne 0) { throw "Fallo SCP de nlp_stream_results" }

scp -i $ResolvedPemPath -o StrictHostKeyChecking=no "${sshTarget}:/home/hadoop/bigdata-kafka/logs/dashboard_alerts_sample.jsonl" (Join-Path $DataDir "flink_alerts_sample.jsonl")
if ($LASTEXITCODE -ne 0) { throw "Fallo SCP de alerts_polarization" }

scp -i $ResolvedPemPath -o StrictHostKeyChecking=no "${sshTarget}:/home/hadoop/bigdata-kafka/logs/dashboard_counts.json" (Join-Path $DataDir "dashboard_counts.json")
if ($LASTEXITCODE -ne 0) { throw "Fallo SCP de dashboard_counts" }

scp -i $ResolvedPemPath -o StrictHostKeyChecking=no "${sshTarget}:/home/hadoop/bigdata-kafka/logs/spark_job2_rules_summary.md" (Join-Path $DataDir "spark_job2_rules_summary.md")
if ($LASTEXITCODE -ne 0) { throw "Fallo SCP de spark_job2_rules_summary" }

scp -i $ResolvedPemPath -o StrictHostKeyChecking=no "${sshTarget}:/home/hadoop/bigdata-kafka/logs/spark_job4_ml_summary.md" (Join-Path $DataDir "spark_job4_ml_summary.md")
if ($LASTEXITCODE -ne 0) { throw "Fallo SCP de spark_job4_ml_summary" }

scp -i $ResolvedPemPath -o StrictHostKeyChecking=no "${sshTarget}:/home/hadoop/bigdata-kafka/logs/spark_job5_hybrid_summary.md" (Join-Path $DataDir "spark_job5_hybrid_summary.md")
if ($LASTEXITCODE -ne 0) { throw "Fallo SCP de spark_job5_hybrid_summary" }

Write-Host "Datos sincronizados en $DataDir"
Write-Host "Nota: dashboard_snapshot.json mantiene estructura enriquecida para la UI; los JSONL copiados sirven como evidencia/live feed local."
