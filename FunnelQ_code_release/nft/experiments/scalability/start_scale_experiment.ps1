[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$Source,
    [Parameter(Mandatory=$true)][string]$RpcUrl,
    [Parameter(Mandatory=$true)][string]$RegistryAddress,
    [string]$ExpectedChainId = "31337",
    [string]$Python = "python",
    [int]$Runs = 10,
    [switch]$Foreground,
    [switch]$Resume
)
$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
$ArgsList = @(
    "-u", (Join-Path $Root "run_scale_experiment.py"),
    "--source", $Source,
    "--rpc-url", $RpcUrl,
    "--registry-address", $RegistryAddress,
    "--expected-chain-id", $ExpectedChainId,
    "--runs", [string]$Runs
)
if ($Resume) { $ArgsList += "--resume" }
if ($Foreground) {
    & $Python @ArgsList
    exit $LASTEXITCODE
}
$Stdout = Join-Path $Root "logs\experiment.stdout.log"
$Stderr = Join-Path $Root "logs\experiment.stderr.log"
$Process = Start-Process -FilePath $Python -ArgumentList $ArgsList -WorkingDirectory $Root -RedirectStandardOutput $Stdout -RedirectStandardError $Stderr -WindowStyle Hidden -PassThru
@{
    pid = $Process.Id
    started_at = (Get-Date).ToString("o")
    command = @($Python) + $ArgsList
    stdout = $Stdout
    stderr = $Stderr
} | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $Root "manifests\background_process.json") -Encoding UTF8
Write-Output "Started isolated FunnelQ scale experiment PID=$($Process.Id)"
