[CmdletBinding()]
param(
    [int]$Bars = 50000,
    [int]$Iterations = 250,
    [string]$Output = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$barsCsv = Join-Path $repoRoot "data\goldm_m5.csv.gz"

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
    & $python -m xpde_ml.train_catboost $barsCsv --output $Output --iterations $Iterations --no-register
    if ($LASTEXITCODE -ne 0) {
        throw "Candidate training failed."
    }
    $manifestPath = Join-Path $repoRoot "$Output\manifest.json"
    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    if ($manifest.eligible_for_shadow -eq $true) {
        & $python (Join-Path $PSScriptRoot "import-colab-artifact.py") `
            (Join-Path $repoRoot $Output)
        if ($LASTEXITCODE -ne 0) {
            throw "Candidate passed training gates but failed load-and-forecast promotion."
        }
        Write-Output "Eligible candidate verified and atomically promoted to artifacts\catboost\latest."
    }
    Write-Output "Candidate artifact: $Output"
    Write-Output "Restart the realtime bridge to load an eligible shadow candidate."
} finally {
    Pop-Location
}
