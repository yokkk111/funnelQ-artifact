[CmdletBinding()]
param(
    [ValidateSet("descriptors","index","vo","chain","verify","all")]
    [string]$Stage = "all",
    [Parameter(Mandatory=$true)][string]$MainDb,
    [Parameter(Mandatory=$true)][string]$TxDb,
    [string]$ClaimsRoot = "",
    [string]$CandidateRootQ1 = "",
    [string]$CandidateRootOther = "",
    [string]$OutRoot = "",
    [string]$ReportsRoot = "",
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
    [ValidateSet("auto","rust","python")]
    [string]$VerifierEngine = "auto",
    [switch]$Force
)
$ErrorActionPreference = "Stop"
$ToolRoot = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($OutRoot)) {
    $OutRoot = Join-Path $ToolRoot "runs\real_selected5"
}
$DescriptorDb = Join-Path $OutRoot "epoch\generic_descriptors.sqlite"
$IndexDir = Join-Path $OutRoot "epoch"
$Packages = Join-Path $OutRoot "packages"
if ([string]::IsNullOrWhiteSpace($ReportsRoot)) {
    $Reports = Join-Path $OutRoot "reports"
} else {
    $Reports = $ReportsRoot
}
$QueryDir = Join-Path $ToolRoot "queries\selected5"
New-Item -ItemType Directory -Force -Path $OutRoot,$Packages,$Reports | Out-Null

function Invoke-Python {
    param([string[]]$Arguments)
    & python (Join-Path $ToolRoot "run_formal_vo.py") @Arguments
    if ($LASTEXITCODE -ne 0) { throw "formal VO command failed" }
}

function Add-ChainCommonArguments {
    param([System.Collections.Generic.List[string]]$Arguments)
    [void]$Arguments.Add("--rpc-url")
    [void]$Arguments.Add($RpcUrl)
    [void]$Arguments.Add("--registry-address")
    [void]$Arguments.Add($RegistryAddress)
    [void]$Arguments.Add("--epoch-id")
    [void]$Arguments.Add($EpochId)
    if (-not [string]::IsNullOrWhiteSpace($ExpectedChainId)) {
        [void]$Arguments.Add("--expected-chain-id")
        [void]$Arguments.Add($ExpectedChainId)
    }
}

$HasRpc = -not [string]::IsNullOrWhiteSpace($RpcUrl)
$HasRegistry = -not [string]::IsNullOrWhiteSpace($RegistryAddress)
if ($HasRpc -xor $HasRegistry) {
    throw "RpcUrl and RegistryAddress must be supplied together."
}

if ($Stage -in @("descriptors","all")) {
    $Args = @("build-descriptors","--db",$MainDb,"--out-db",$DescriptorDb)
    if ($Force) { $Args += "--force" }
    Invoke-Python $Args
}
if ($Stage -in @("index","all")) {
    $Args = @("build-index","--db",$MainDb,"--tx-db",$TxDb,
              "--descriptor-db",$DescriptorDb,"--out-dir",$IndexDir)
    if ($Force) { $Args += "--force" }
    Invoke-Python $Args
}
$QueryIds = @(
  "Q1_RAPID_SAME_NFT_ROUNDTRIP",
  "Q2_CROSS_NFT_RECIPROCAL",
  "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
  "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
  "Q5_TWO_NFT_RETURN_CYCLE"
)
if ($Stage -in @("vo","all")) {
    if ([string]::IsNullOrWhiteSpace($CandidateRootQ1) -or [string]::IsNullOrWhiteSpace($CandidateRootOther)) {
        throw "Stage vo/all requires -CandidateRootQ1 and -CandidateRootOther."
    }
    foreach ($QueryId in $QueryIds) {
        $Hint = if ($QueryId.StartsWith("Q1_")) {
            Join-Path $CandidateRootQ1 "cand2_closures.csv"
        } else {
            Join-Path (Join-Path $CandidateRootOther $QueryId) "cand2_closures.csv"
        }
        $Args = @("build-vo","--index-dir",$IndexDir,"--db",$MainDb,"--tx-db",$TxDb,
                  "--query",(Join-Path $QueryDir "$QueryId.json"),
                  "--candidate-hint",$Hint,"--variant","full","--out-dir",(Join-Path $Packages $QueryId))
        if ($Force) { $Args += "--force" }
        Invoke-Python $Args
    }
}
if ($Stage -in @("chain","all")) {
    if (-not ($HasRpc -and $HasRegistry)) {
        if ($Stage -eq "chain") {
            throw "Stage chain requires -RpcUrl and -RegistryAddress."
        }
        Write-Warning "No RPC/registry supplied; Stage all skips chain commit and performs a local-root dry-run verification."
    } else {
        $ChainReport = Join-Path $IndexDir "chain_commit.json"
        $Args = [System.Collections.Generic.List[string]]::new()
        [void]$Args.Add("commit-chain-root")
        Add-ChainCommonArguments $Args
        [void]$Args.Add("--manifest")
        [void]$Args.Add((Join-Path $IndexDir "epoch_manifest.json"))
        [void]$Args.Add("--read-signature")
        [void]$Args.Add($ChainReadSignature)
        [void]$Args.Add("--write-signature")
        [void]$Args.Add($ChainWriteSignature)
        [void]$Args.Add("--transaction-timeout-seconds")
        [void]$Args.Add([string]$TransactionTimeoutSeconds)
        if (-not [string]::IsNullOrWhiteSpace($FromAddress)) {
            [void]$Args.Add("--from-address")
            [void]$Args.Add($FromAddress)
        }
        if ($Gas -gt 0) {
            [void]$Args.Add("--gas")
            [void]$Args.Add([string]$Gas)
        }
        [void]$Args.Add("--out-json")
        [void]$Args.Add($ChainReport)
        Invoke-Python $Args.ToArray()
        Write-Host "Chain commit/readback report: $ChainReport"
    }
}
if ($Stage -in @("verify","all")) {
    if ([string]::IsNullOrWhiteSpace($ClaimsRoot)) {
        throw "Stage verify/all requires -ClaimsRoot."
    }
    $UseLiveChain = $HasRpc -and $HasRegistry
    if (-not $UseLiveChain -and [string]::IsNullOrWhiteSpace($ExpectedEpochRoot)) {
        $Epoch = Get-Content (Join-Path $IndexDir "epoch_manifest.json") -Raw | ConvertFrom-Json
        $ExpectedEpochRoot = $Epoch.epochRoot
        Write-Warning "Using the locally built EpochRoot for a dry run. Supply -RpcUrl and -RegistryAddress for the final chain-bound experiment."
    }
    foreach ($QueryId in $QueryIds) {
        $Args = [System.Collections.Generic.List[string]]::new()
        [void]$Args.Add("verify")
        [void]$Args.Add("--package")
        [void]$Args.Add((Join-Path $Packages $QueryId))
        [void]$Args.Add("--query")
        [void]$Args.Add((Join-Path $QueryDir "$QueryId.json"))
        [void]$Args.Add("--instances")
        [void]$Args.Add((Join-Path (Join-Path $ClaimsRoot $QueryId) "instances.csv"))
        if ($UseLiveChain) {
            Add-ChainCommonArguments $Args
            [void]$Args.Add("--chain-read-signature")
            [void]$Args.Add($ChainReadSignature)
            [void]$Args.Add("--chain-block-tag")
            [void]$Args.Add($ChainBlockTag)
            [void]$Args.Add("--rpc-timeout-seconds")
            [void]$Args.Add([string]$RpcTimeoutSeconds)
        } else {
            [void]$Args.Add("--expected-epoch-root")
            [void]$Args.Add($ExpectedEpochRoot)
        }
        [void]$Args.Add("--engine")
        [void]$Args.Add($VerifierEngine)
        [void]$Args.Add("--out-json")
        [void]$Args.Add((Join-Path $Reports "$QueryId.json"))
        [void]$Args.Add("--work-dir")
        [void]$Args.Add((Join-Path $Reports "$QueryId.work"))
        Invoke-Python $Args.ToArray()
    }
    $MetricsCsv = Join-Path $Reports "selected5_layered_metrics.csv"
    & python (Join-Path $ToolRoot "summarize_metrics.py") `
        --packages-dir $Packages `
        --reports-dir $Reports `
        --result-root $ClaimsRoot `
        --out-csv $MetricsCsv
    if ($LASTEXITCODE -ne 0) { throw "metric summarization failed" }
    Write-Host "Layered metrics: $MetricsCsv"
}
