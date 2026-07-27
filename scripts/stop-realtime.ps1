[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$statePath = Join-Path $repoRoot "work\realtime-processes.json"

if (-not (Test-Path -LiteralPath $statePath)) {
    Write-Output "No XPDE process state file was found."
    return
}

$tracked = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
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

foreach ($entry in $tracked) {
    $process = Get-Process -Id ([int]$entry.pid) -ErrorAction SilentlyContinue
    if (-not $process) {
        continue
    }
    $expected = [DateTime]::Parse($entry.started_at).ToUniversalTime()
    $actual = $process.StartTime.ToUniversalTime()
    if ([Math]::Abs(($actual - $expected).TotalSeconds) -gt 2) {
        Write-Warning "Skipped reused PID $($entry.pid) for $($entry.name)."
        continue
    }
    $descendants = @(Get-DescendantIds -ParentId $process.Id)
    foreach ($processId in $descendants) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
    Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    Write-Output "Stopped $($entry.name) (PID $($entry.pid))."
}

Remove-Item -LiteralPath $statePath -Force
