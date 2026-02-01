#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Get-WorkspaceVersion {
    $inPkg = $false
    foreach ($line in Get-Content -LiteralPath (Join-Path $Root "Cargo.toml")) {
        if ($line -match '^\[workspace\.package\]') {
            $inPkg = $true
            continue
        }
        if ($inPkg -and $line -match '^\[') {
            $inPkg = $false
        }
        if ($inPkg -and $line -match '^version\s*=\s*"([^"]+)"') {
            return $Matches[1]
        }
    }
    throw "Failed to determine workspace version from Cargo.toml"
}

function Get-PlatformArch {
    $arch = $null
    try {
        $osArch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
        if ($null -ne $osArch) {
            $arch = $osArch.ToString()
        }
    }
    catch {
        # Fall back to env vars below.
    }

    if ([string]::IsNullOrWhiteSpace($arch)) {
        # Some Windows environments (notably 32-bit PowerShell or older runtimes) can fail to
        # provide RuntimeInformation.OSArchitecture. Fall back to environment variables.
        $arch = $env:PROCESSOR_ARCHITECTURE
        if ($arch -eq "x86" -and $env:PROCESSOR_ARCHITEW6432) {
            $arch = $env:PROCESSOR_ARCHITEW6432
        }
    }

    if ([string]::IsNullOrWhiteSpace($arch)) {
        throw "Failed to determine platform architecture (RuntimeInformation + PROCESSOR_ARCHITECTURE are unavailable)."
    }
    switch ($arch) {
        "X64" { return "amd64" }
        "AMD64" { return "amd64" }
        "Arm64" { return "arm64" }
        "ARM64" { return "arm64" }
        "x86" { return "x86" }
        default { return $arch.ToLowerInvariant() }
    }
}

$DistDir = $env:DIST_DIR
if ([string]::IsNullOrWhiteSpace($DistDir)) {
    $DistDir = Join-Path $Root "dist"
}
New-Item -ItemType Directory -Force -Path $DistDir | Out-Null

Write-Host "Building WesoForge CLI (wesoforge)..." -ForegroundColor Cyan
cargo build -p bbr-client --release --features prod-backend

$Version = Get-WorkspaceVersion
$Arch = Get-PlatformArch

$TargetDir = $env:CARGO_TARGET_DIR
if ([string]::IsNullOrWhiteSpace($TargetDir)) {
    $TargetDir = Join-Path $Root "target"
}

$BinSrc = Join-Path $TargetDir "release\\wesoforge.exe"
if (!(Test-Path -LiteralPath $BinSrc)) {
    throw "Expected binary not found at: $BinSrc"
}

$BinDst = Join-Path $DistDir ("WesoForge-cli_Windows_{0}_{1}.exe" -f $Version, $Arch)
Copy-Item -Force -LiteralPath $BinSrc -Destination $BinDst

# MPIR runtime DLLs (required by chiavdf on Windows).
$MpirDir = Join-Path $Root "chiavdf\\mpir_gc_x64"
if (!(Test-Path -LiteralPath $MpirDir)) {
    throw "MPIR directory not found at: $MpirDir"
}
Copy-Item -Force -Path (Join-Path $MpirDir "mpir*.dll") -Destination $DistDir

Write-Host "Wrote: $BinDst" -ForegroundColor Green
