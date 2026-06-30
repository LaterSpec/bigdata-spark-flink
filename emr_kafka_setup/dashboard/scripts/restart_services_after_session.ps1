$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BashCandidates = @(
  "C:\Program Files\Git\bin\bash.exe",
  "C:\Program Files (x86)\Git\bin\bash.exe"
)
$Bash = $BashCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not $Bash) {
  throw "Git Bash no está instalado. Ejecuta restart_services_after_session.sh desde Bash."
}

$BashScript = $ScriptDir.Replace("\", "/")
if ($BashScript -match "^([A-Za-z]):/(.*)$") {
  $BashScript = "/$($Matches[1].ToLower())/$($Matches[2])"
}

& $Bash "$BashScript/restart_services_after_session.sh" @args
if ($LASTEXITCODE -ne 0) {
  throw "La reanudación distribuida terminó con código $LASTEXITCODE."
}
