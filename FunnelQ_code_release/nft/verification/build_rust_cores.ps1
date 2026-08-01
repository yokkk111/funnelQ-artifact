[CmdletBinding()]
param(
    [switch]$Clean,
    [switch]$VerifierOnly
)
$ErrorActionPreference = "Stop"
$ToolRoot = $PSScriptRoot
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "Cargo was not found. Install the Rust toolchain and reopen PowerShell."
}

$DescriptorManifest = Join-Path $ToolRoot "rust_descriptor_builder\Cargo.toml"
$VerifierManifest = Join-Path $ToolRoot "rust_verifier_core\Cargo.toml"

if ($Clean) {
    if (-not $VerifierOnly) {
        & cargo clean --manifest-path $DescriptorManifest
        if ($LASTEXITCODE -ne 0) { throw "cargo clean failed for descriptor builder" }
    }
    & cargo clean --manifest-path $VerifierManifest
    if ($LASTEXITCODE -ne 0) { throw "cargo clean failed for verifier core" }
}

if (-not $VerifierOnly) {
    & cargo build --release --locked --manifest-path $DescriptorManifest
    if ($LASTEXITCODE -ne 0) { throw "Rust descriptor builder compilation failed" }
}
& cargo build --release --manifest-path $VerifierManifest
if ($LASTEXITCODE -ne 0) { throw "Rust verifier core compilation failed" }

$BinDir = Join-Path $ToolRoot "bin"
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
$Suffix = if ($IsWindows -or $env:OS -eq "Windows_NT") { ".exe" } else { "" }

if (-not $VerifierOnly) {
    $Descriptor = Join-Path $ToolRoot "rust_descriptor_builder\target\release\formal_vo_descriptor_builder$Suffix"
    if (-not (Test-Path $Descriptor)) { throw "Compiled descriptor binary was not found: $Descriptor" }
    Copy-Item -Force $Descriptor (Join-Path $BinDir "formal_vo_descriptor_builder$Suffix")
}

$Verifier = Join-Path $ToolRoot "rust_verifier_core\target\release\formal_vo_verifier_core$Suffix"
if (-not (Test-Path $Verifier)) { throw "Compiled verifier binary was not found: $Verifier" }
Copy-Item -Force $Verifier (Join-Path $BinDir "formal_vo_verifier_core$Suffix")
Write-Host "Rust verifier built and copied to $BinDir"
if (-not $VerifierOnly) {
    Write-Host "Rust descriptor builder also built and copied."
}
