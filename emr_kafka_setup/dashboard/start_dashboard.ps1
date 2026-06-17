$ErrorActionPreference = "Stop"
$DashboardRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $DashboardRoot
Write-Host "Dashboard local: http://localhost:8787"
if (Get-Command node -ErrorAction SilentlyContinue) {
  node .\server.js
} elseif (Get-Command py -ErrorAction SilentlyContinue) {
  py -m http.server 8787
} else {
  Write-Error "No se encontro Node ni Python. Sirve esta carpeta con cualquier static server."
}
