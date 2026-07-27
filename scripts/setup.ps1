[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

Push-Location $repoRoot
try {
    cargo build --workspace
    npm install --no-audit --no-fund
    npm run build
    if (-not (Test-Path -LiteralPath $venvPython)) {
        python -m venv .venv
    }
    & $venvPython -m pip install --disable-pip-version-check -e ".\ml[dev,mt5]"
    Write-Output "XPDE local dependencies are ready."
} finally {
    Pop-Location
}
