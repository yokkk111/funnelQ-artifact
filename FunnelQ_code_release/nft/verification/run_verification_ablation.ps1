[CmdletBinding()]
param(
    [int]$Runs = 10,
    [Parameter(Mandatory=$true)][string]$MainDb,
    [Parameter(Mandatory=$true)][string]$TxDb,
    [string]$IndexDir = "",
    [Parameter(Mandatory=$true)][string]$ClaimsRoot,
    [string]$OutRoot = "",
    [string]$ReusePackagesFrom = "",
    [ValidateSet("auto","rust","python")][string]$Engine = "auto",
    [string]$QueryIds = "Q1_RAPID_SAME_NFT_ROUNDTRIP,Q2_CROSS_NFT_RECIPROCAL,Q3_SAME_NFT_THREE_ACTOR_CYCLE,Q4_THREE_NFT_THREE_ACTOR_CYCLE,Q5_TWO_NFT_RETURN_CYCLE",
    [string]$Variants = "full,no_anchor,no_refined,no_pruning",
    [string]$ExpectedEpochRoot = "",
    [string]$RpcUrl = "",
    [string]$RegistryAddress = "",
    [string]$EpochId = "auto",
    [string]$ChainReadSignature = "getEpochRoot(bytes32)",
    [string]$ChainWriteSignature = "commitEpochRoot(bytes32,bytes32)",
    [string]$ChainBlockTag = "latest",
    [string]$FromAddress = "",
    [string]$ExpectedChainId = "",
    [int]$RpcTimeoutSeconds = 15,
    [int]$TransactionTimeoutSeconds = 60,
    [int]$Gas = 0,
    [switch]$CommitChainRoot,
    [switch]$AllowLocalRootDryRun,
    [int]$VerifyTimeoutSec = 600,
    [int]$BuildTimeoutSec = 3600,
    [int]$MaxRssMb = 18000,
    [switch]$ForcePackages,
    [switch]$Resume,
    [switch]$AllowIncomplete
)
$ErrorActionPreference = "Stop"
$ToolRoot = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($IndexDir) -and [string]::IsNullOrWhiteSpace($ReusePackagesFrom)) {
    $IndexDir = Join-Path $ToolRoot "runs\real_selected5\epoch"
}
if ([string]::IsNullOrWhiteSpace($OutRoot)) {
    $Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $OutRoot = Join-Path $ToolRoot "runs\verification_ablation_${Runs}x_$Stamp"
}
if ($ForcePackages -and -not [string]::IsNullOrWhiteSpace($ReusePackagesFrom)) {
    throw "-ForcePackages cannot be combined with -ReusePackagesFrom"
}
$HasRpc = -not [string]::IsNullOrWhiteSpace($RpcUrl)
$HasRegistry = -not [string]::IsNullOrWhiteSpace($RegistryAddress)
if ($HasRpc -xor $HasRegistry) {
    throw "RpcUrl and RegistryAddress must be supplied together."
}
if (-not ($HasRpc -and $HasRegistry) -and -not $AllowLocalRootDryRun) {
    throw "Formal verification ablation requires RpcUrl and RegistryAddress. Use -AllowLocalRootDryRun only for diagnostics."
}
if ($CommitChainRoot -and -not ($HasRpc -and $HasRegistry)) {
    throw "CommitChainRoot requires RpcUrl and RegistryAddress."
}

$RunArgs = @(
  (Join-Path $ToolRoot "run_verification_ablation.py"),
  "--db",$MainDb,"--tx-db",$TxDb,
  "--claims-root",$ClaimsRoot,"--out-root",$OutRoot,"--runs",[string]$Runs,
  "--query-ids",$QueryIds,"--variants",$Variants,"--engine",$Engine,
  "--verify-timeout-sec",[string]$VerifyTimeoutSec,
  "--build-timeout-sec",[string]$BuildTimeoutSec,
  "--max-rss-mb",[string]$MaxRssMb
)
if (-not [string]::IsNullOrWhiteSpace($IndexDir)) {
    $RunArgs += @("--index-dir", $IndexDir)
}
if ($ForcePackages) { $RunArgs += "--force-packages" }
if ($Resume) { $RunArgs += "--resume" }
if (-not [string]::IsNullOrWhiteSpace($ReusePackagesFrom)) {
    $RunArgs += @("--reuse-packages-from", $ReusePackagesFrom)
}
if ($HasRpc -and $HasRegistry) {
    $RunArgs += @(
        "--rpc-url",$RpcUrl,
        "--registry-address",$RegistryAddress,
        "--epoch-id",$EpochId,
        "--chain-read-signature",$ChainReadSignature,
        "--chain-write-signature",$ChainWriteSignature,
        "--chain-block-tag",$ChainBlockTag,
        "--rpc-timeout-seconds",[string]$RpcTimeoutSeconds,
        "--transaction-timeout-seconds",[string]$TransactionTimeoutSeconds
    )
    if (-not [string]::IsNullOrWhiteSpace($ExpectedChainId)) {
        $RunArgs += @("--expected-chain-id",$ExpectedChainId)
    }
    if (-not [string]::IsNullOrWhiteSpace($FromAddress)) {
        $RunArgs += @("--from-address",$FromAddress)
    }
    if ($Gas -gt 0) { $RunArgs += @("--gas",[string]$Gas) }
    if ($CommitChainRoot) { $RunArgs += "--commit-chain-root" }
} else {
    $RunArgs += "--allow-local-root-dry-run"
    if (-not [string]::IsNullOrWhiteSpace($ExpectedEpochRoot)) {
        $RunArgs += @("--expected-epoch-root",$ExpectedEpochRoot)
    }
}
& python @RunArgs
$Exit = $LASTEXITCODE
Write-Host "Verification ablation output: $OutRoot"
if (-not [string]::IsNullOrWhiteSpace($ReusePackagesFrom)) {
    Write-Host "Reused VO packages: $ReusePackagesFrom"
}
if ($Exit -ne 0) {
    $Message = "Verification ablation did not complete all requested runs; inspect report/logs in $OutRoot"
    if ($AllowIncomplete) { Write-Warning $Message } else { throw $Message }
}
