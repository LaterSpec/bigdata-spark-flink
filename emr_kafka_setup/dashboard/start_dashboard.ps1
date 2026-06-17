$ErrorActionPreference = "Stop"
$DashboardRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $DashboardRoot

$Port = if ($env:PORT) { $env:PORT } else { "8787" }
$BindHost = if ($env:HOST) { $env:HOST } else { "127.0.0.1" }

Write-Host "Dashboard local: http://${BindHost}:${Port}"

if (Get-Command node -ErrorAction SilentlyContinue) {
  $env:PORT = $Port
  $env:HOST = $BindHost
  node "$DashboardRoot\server.js"
} elseif (Get-Command py -ErrorAction SilentlyContinue) {
  Write-Warning "Node no encontrado. Sirviendo solo archivos estaticos con Python; los endpoints /api/* (Conectar AWS, Spark, deltas) NO funcionaran. Instala Node para el dashboard completo."
  py -m http.server $Port --bind $BindHost --directory $DashboardRoot
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
  Write-Warning "Node no encontrado. Sirviendo solo archivos estaticos con Python; los endpoints /api/* (Conectar AWS, Spark, deltas) NO funcionaran. Instala Node para el dashboard completo."
  python -m http.server $Port --bind $BindHost --directory $DashboardRoot
} else {
  Write-Error "No se encontro Node ni Python. Sirve esta carpeta con cualquier static server."
}
