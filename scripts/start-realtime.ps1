[CmdletBinding()]
param(
    [double]$Interval = 1.0
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$workDir = Join-Path $repoRoot "work"
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$xuva = Join-Path $env:USERPROFILE ".local\bin\xuva.exe"
$candidateDir = Join-Path $repoRoot "artifacts\catboost\latest"
$requiredCandidateFiles = @(
    "checksums.sha256",
    "manifest.json",
    "evaluation.json",
    "model_card.md",
    "direction.cbm",
    "quantile_h1.cbm",
    "quantile_h3.cbm",
    "quantile_h6.cbm",
    "quantile_h12.cbm",
    "barrier_long_h3.cbm",
    "barrier_short_h3.cbm",
    "mfe_long_h3.cbm",
    "mae_long_h3.cbm",
    "mfe_short_h3.cbm",
    "mae_short_h3.cbm"
)

if (-not (Test-Path -LiteralPath $python)) {
    throw "Python environment is missing. Run .\scripts\setup.ps1 first."
}
if (-not (Test-Path -LiteralPath $xuva)) {
    throw "XUVA is missing from $xuva."
}

New-Item -ItemType Directory -Path $workDir -Force | Out-Null
$env:XUVA_ROUTE = "raw"
$started = @()

function Test-LocalUrl {
    param([Parameter(Mandatory)][string]$Url)
    try {
        $request = [Net.HttpWebRequest]::Create($Url)
        $request.Timeout = 1500
        $request.Method = "GET"
        $response = $request.GetResponse()
        try {
            return [int]$response.StatusCode -ge 200 -and [int]$response.StatusCode -lt 500
        } finally {
            $response.Dispose()
        }
    } catch {
        return $false
    }
}

function Start-TrackedProcess {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string[]]$ArgumentList
    )
    $stdout = Join-Path $workDir "$Name.stdout.log"
    $stderr = Join-Path $workDir "$Name.stderr.log"
    $process = Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $repoRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru
    $script:started += [pscustomobject]@{
        name = $Name
        pid = $process.Id
        started_at = $process.StartTime.ToUniversalTime().ToString("o")
    }
    return $process
}

if (-not (Test-LocalUrl "http://127.0.0.1:8787/health")) {
    Start-TrackedProcess `
        -Name "core" `
        -FilePath $xuva `
        -ArgumentList @("--environment", "windows-only", "cargo", "run", "-p", "xpde-server") |
        Out-Null
}

if (-not (Test-LocalUrl "http://localhost:3000/")) {
    Start-TrackedProcess `
        -Name "dashboard" `
        -FilePath $xuva `
        -ArgumentList @("--environment", "windows-only", "node", "scripts\dashboard-server.mjs") |
        Out-Null
}

$deadline = (Get-Date).AddSeconds(90)
while (-not (Test-LocalUrl "http://127.0.0.1:8787/health")) {
    if ((Get-Date) -gt $deadline) {
        throw "XPDE core did not become healthy. Check work\core.stderr.log."
    }
    Start-Sleep -Milliseconds 500
}

$existingBridge = Get-CimInstance Win32_Process |
    Where-Object {
        $_.CommandLine -match "xpde_ml\.mt5_bridge" -and
        $_.ExecutablePath -eq $python
    } |
    Select-Object -First 1

if ($existingBridge) {
    Write-Output "MT5 realtime bridge is already running (PID $($existingBridge.ProcessId))."
} else {
    $intervalText = $Interval.ToString([Globalization.CultureInfo]::InvariantCulture)
    $bridgeArguments = @("-m", "xpde_ml.mt5_bridge", "--interval", $intervalText)
    if (Test-Path -LiteralPath (Join-Path $candidateDir "manifest.json")) {
        $manifest = Get-Content -Raw -LiteralPath (Join-Path $candidateDir "manifest.json") |
            ConvertFrom-Json
        $hasRequiredFiles = @(
            $requiredCandidateFiles |
                Where-Object { -not (Test-Path -LiteralPath (Join-Path $candidateDir $_)) }
        ).Count -eq 0
        $gateProperties = @($manifest.eligibility_gates.PSObject.Properties)
        $allGatesPassed = (
            $gateProperties.Count -gt 0 -and
            @($gateProperties | Where-Object { $_.Value -ne $true }).Count -eq 0
        )
        if (
            $manifest.eligible_for_shadow -eq $true -and
            [int]$manifest.schema_version -eq 3 -and
            [int]$manifest.eligibility_gate_version -ge 2 -and
            $manifest.training_mode -eq "candidate" -and
            $manifest.barrier_spec.id -eq "atr-1.25tp-1.00sl-h3-v1" -and
            [int]$manifest.barrier_spec.horizon_bars -eq 3 -and
            $allGatesPassed -and
            $hasRequiredFiles
        ) {
            $bridgeArguments += @("--model-dir", $candidateDir)
        }
    }
    $bridge = Start-TrackedProcess `
        -Name "mt5-bridge" `
        -FilePath $python `
        -ArgumentList $bridgeArguments
    Write-Output "MT5 realtime bridge started (PID $($bridge.Id))."
}

if ($started.Count -gt 0) {
    $started |
        ConvertTo-Json |
        Set-Content -LiteralPath (Join-Path $workDir "realtime-processes.json") -Encoding UTF8
}

Write-Output "Dashboard: http://localhost:3000/"
Write-Output "Core:      http://127.0.0.1:8787/health"
