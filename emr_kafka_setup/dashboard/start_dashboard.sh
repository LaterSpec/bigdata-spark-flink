#!/usr/bin/env bash
set -euo pipefail

DashboardRoot="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DashboardRoot"

Port="${PORT:-8787}"
BindHost="${HOST:-127.0.0.1}"

echo "Dashboard local: http://${BindHost}:${Port}"

static_warning() {
  echo "Node no encontrado. Sirviendo solo archivos estaticos con Python; los endpoints /api/* (Conectar AWS, Spark, deltas) NO funcionaran. Instala Node para el dashboard completo." >&2
}

if command -v node >/dev/null 2>&1; then
  PORT="$Port" HOST="$BindHost" node "$DashboardRoot/server.js"
elif command -v python3 >/dev/null 2>&1; then
  static_warning
  python3 -m http.server "$Port" --bind "$BindHost" --directory "$DashboardRoot"
elif command -v python >/dev/null 2>&1; then
  static_warning
  python -m http.server "$Port" --bind "$BindHost" --directory "$DashboardRoot"
else
  echo "No se encontro Node ni Python. Sirve esta carpeta con cualquier static server." >&2
  exit 1
fi
