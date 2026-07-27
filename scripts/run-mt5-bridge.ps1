[CmdletBinding()]
param(
    [switch]$Once,
    [double]$Interval = 1.0
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Python environment is missing. Run .\scripts\setup.ps1 first."
}

$arguments = @(
    "-m", "xpde_ml.mt5_bridge",
    "--interval", $Interval.ToString([Globalization.CultureInfo]::InvariantCulture)
)
if ($Once) {
    $arguments += "--once"
}

Push-Location $repoRoot
try {
    & $python @arguments
} finally {
    Pop-Location
}
