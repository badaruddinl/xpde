[CmdletBinding()]
param(
    [string]$Bind = "127.0.0.1:8787",
    [string]$Database = "data/xpde.sqlite"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$env:XPDE_BIND = $Bind
$env:XPDE_DB_PATH = Join-Path $repoRoot $Database

Push-Location $repoRoot
try {
    cargo run -p xpde-server
} finally {
    Pop-Location
}
