[CmdletBinding()]
param(
    [int]$Bars = 50000,
    [int]$Iterations = 250,
    [string]$Output = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$barsCsv = Join-Path $repoRoot "data\goldm_m5.csv"

Push-Location $repoRoot
try {
    if ([string]::IsNullOrWhiteSpace($Output)) {
        $runStamp = [DateTime]::UtcNow.ToString("yyyyMMdd-HHmmss")
        $Output = "artifacts\catboost\runs\$runStamp"
    }
    & $python -m pip install --disable-pip-version-check -e ".\ml[train,mt5]"
    & $python -m xpde_ml.backfill_mt5 --bars $Bars --output $barsCsv
    if ($LASTEXITCODE -ne 0) {
        throw "MT5 historical backfill failed."
    }
    & $python -m xpde_ml.train_catboost $barsCsv --output $Output --iterations $Iterations
    if ($LASTEXITCODE -ne 0) {
        throw "Candidate training failed."
    }
    $manifestPath = Join-Path $repoRoot "$Output\manifest.json"
    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    if ($manifest.eligible_for_shadow -eq $true) {
        $latest = Join-Path $repoRoot "artifacts\catboost\latest"
        if (Test-Path -LiteralPath $latest) {
            Remove-Item -LiteralPath $latest -Recurse -Force
        }
        Copy-Item -LiteralPath (Join-Path $repoRoot $Output) -Destination $latest -Recurse
        Write-Output "Eligible candidate copied to artifacts\catboost\latest."
    }
    Write-Output "Candidate artifact: $Output"
    Write-Output "Restart the realtime bridge to load an eligible shadow candidate."
} finally {
    Pop-Location
}
