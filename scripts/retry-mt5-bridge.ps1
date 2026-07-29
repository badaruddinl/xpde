[CmdletBinding()]
param(
    [double]$Interval = 1.0
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$workDir = Join-Path $repoRoot "work"
$statePath = Join-Path $workDir "realtime-processes.json"
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
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

New-Item -ItemType Directory -Path $workDir -Force | Out-Null
$allProcesses = @(Get-CimInstance Win32_Process)

function Get-DescendantIds {
    param([Parameter(Mandatory)][int]$ParentId)

    $children = @($allProcesses | Where-Object { $_.ParentProcessId -eq $ParentId })
    $result = @()
    foreach ($child in $children) {
        $result += Get-DescendantIds -ParentId $child.ProcessId
        $result += [int]$child.ProcessId
    }
    return $result
}

$existingBridges = @(
    $allProcesses |
        Where-Object {
            $_.CommandLine -match "xpde_ml\.mt5_bridge" -and
            $_.ExecutablePath -eq $python
        }
)
$restarted = $existingBridges.Count -gt 0
foreach ($bridge in $existingBridges) {
    foreach ($processId in @(Get-DescendantIds -ParentId $bridge.ProcessId)) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
    Stop-Process -Id $bridge.ProcessId -Force -ErrorAction SilentlyContinue
}
if ($restarted) {
    Start-Sleep -Milliseconds 250
}

$bridgeArguments = @(
    "-m",
    "xpde_ml.mt5_bridge",
    "--interval",
    $Interval.ToString([Globalization.CultureInfo]::InvariantCulture)
)
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
        [int]$manifest.eligibility_gate_version -ge 3 -and
        $manifest.training_mode -eq "candidate" -and
        $manifest.barrier_spec.id -eq "atr-1.25tp-1.00sl-h3-executable-v4" -and
        $manifest.executable_side_contract.id -eq "bid-entry-exit-long-ask-exit-short-tick-sequence-v3" -and
        [int]$manifest.barrier_spec.horizon_bars -eq 3 -and
        $allGatesPassed -and
        $hasRequiredFiles
    ) {
        $bridgeArguments += @("--model-dir", $candidateDir)
    }
}

$stdout = Join-Path $workDir "mt5-bridge.stdout.log"
$stderr = Join-Path $workDir "mt5-bridge.stderr.log"
$process = Start-Process `
    -FilePath $python `
    -ArgumentList $bridgeArguments `
    -WorkingDirectory $repoRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -PassThru
Start-Sleep -Milliseconds 750
$process.Refresh()
if ($process.HasExited) {
    $details = if (Test-Path -LiteralPath $stderr) {
        (Get-Content -LiteralPath $stderr -Tail 12) -join "`n"
    } else {
        "bridge exited before producing an error log"
    }
    [pscustomobject]@{
        status = "failed"
        error = $details
    } | ConvertTo-Json -Compress
    exit 1
}

$tracked = if (Test-Path -LiteralPath $statePath) {
    @(Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json)
} else {
    @()
}
$entry = [pscustomobject]@{
    name = "mt5-bridge"
    pid = $process.Id
    started_at = $process.StartTime.ToUniversalTime().ToString("o")
}
$nextState = @($tracked | Where-Object { $_.name -ne "mt5-bridge" }) + @($entry)
ConvertTo-Json -InputObject @($nextState) |
    Set-Content -LiteralPath $statePath -Encoding UTF8

[pscustomobject]@{
    status = if ($restarted) { "restarted" } else { "started" }
    pid = $process.Id
} | ConvertTo-Json -Compress
exit 0
