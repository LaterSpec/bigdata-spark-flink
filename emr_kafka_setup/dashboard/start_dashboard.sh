#!/usr/bin/env bash
set -euo pipefail

DashboardRoot="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DashboardRoot"

Port="${PORT:-8787}"

echo "Dashboard local: http://localhost:${Port}"

if command -v node >/dev/null 2>&1; then
  PORT="$Port" node "$DashboardRoot/server.js"
elif command -v python3 >/dev/null 2>&1; then
  python3 -m http.server "$Port" --directory "$DashboardRoot"
elif command -v python >/dev/null 2>&1; then
  python -m http.server "$Port" --directory "$DashboardRoot"
else
  echo "No se encontro Node ni Python. Sirve esta carpeta con cualquier static server." >&2
  exit 1
fi
