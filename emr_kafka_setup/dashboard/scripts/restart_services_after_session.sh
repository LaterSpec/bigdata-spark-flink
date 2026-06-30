#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Reanudando servicios distribuidos sin borrar topics ni offsets Kafka."
exec "$ScriptDir/bootstrap_emr_streaming.sh" --no-reset-topics "$@"
