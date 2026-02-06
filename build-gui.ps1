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

function Get-WebView2ArchFolder([string] $arch) {
    switch ($arch) {
        "amd64" { return "x64" }
        "arm64" { return "arm64" }
        "x86" { return "x86" }
        default { throw "Unsupported architecture for WebView2Loader.dll lookup: $arch" }
    }
}

function Ensure-IconIco {
    $png = Join-Path $Root "crates/client-gui/icons/icon.png"
    $ico = Join-Path $Root "crates/client-gui/icons/icon.ico"

    if (Test-Path -LiteralPath $ico) {
        return
    }
    if (!(Test-Path -LiteralPath $png)) {
        throw "Missing icon source PNG at: $png"
    }

    Write-Host "Generating missing icon.ico from icon.png..." -ForegroundColor Cyan

    Add-Type -AssemblyName System.Drawing

    $img = [System.Drawing.Image]::FromFile($png)
    try {
        $size = 256
        $bmp = New-Object System.Drawing.Bitmap $size, $size
        try {
            $g = [System.Drawing.Graphics]::FromImage($bmp)
            try {
                $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
                $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
                $g.CompositingQuality = [System.Drawing.Drawing2D.CompositingQuality]::HighQuality
                $g.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
                $g.DrawImage($img, 0, 0, $size, $size)
            }
            finally {
                $g.Dispose()
            }

            $hicon = $bmp.GetHicon()
            try {
                $icon = [System.Drawing.Icon]::FromHandle($hicon)
                try {
                    $fs = New-Object System.IO.FileStream($ico, [System.IO.FileMode]::Create)
                    try {
                        $icon.Save($fs)
                    }
                    finally {
                        $fs.Dispose()
                    }
                }
                finally {
                    $icon.Dispose()
                }
            }
            finally {
                Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class Win32 {
  [DllImport("user32.dll", CharSet = CharSet.Auto)]
  public static extern bool DestroyIcon(IntPtr handle);
}
"@
                [Win32]::DestroyIcon($hicon) | Out-Null
            }
        }
        finally {
            $bmp.Dispose()
        }
    }
    finally {
        $img.Dispose()
    }
}

function Ensure-UiDependencies {
    $UiDir = Join-Path $Root "ui"
    if (!(Test-Path -LiteralPath (Join-Path $UiDir "package.json"))) {
        throw "GUI ui/package.json not found at: $UiDir"
    }

    if ($env:BBR_SKIP_PNPM_INSTALL -eq "1") {
        return
    }

    if (!(Get-Command pnpm -ErrorAction SilentlyContinue)) {
        throw "pnpm not found (needed to build the GUI frontend)."
    }

    $NodeModules = Join-Path $UiDir "node_modules"
    if (!(Test-Path -LiteralPath $NodeModules)) {
        Write-Host "Installing GUI frontend dependencies (ui/node_modules missing)..." -ForegroundColor Cyan
        & pnpm -C $UiDir install
        if ($LASTEXITCODE -ne 0) {
            throw "pnpm install failed (exit code: $LASTEXITCODE)"
        }
    }
}

$DistDir = $env:DIST_DIR
if ([string]::IsNullOrWhiteSpace($DistDir)) {
    $DistDir = Join-Path $Root "dist"
}
New-Item -ItemType Directory -Force -Path $DistDir | Out-Null

$StageRoot = Join-Path $DistDir "_staging_gui"
if (Test-Path -LiteralPath $StageRoot) {
    Remove-Item -Recurse -Force -LiteralPath $StageRoot
}

$AppDirName = "WesoForge"
$AppDir = Join-Path $StageRoot $AppDirName
New-Item -ItemType Directory -Force -Path $AppDir | Out-Null

Ensure-IconIco
Ensure-UiDependencies

if ($env:BBR_SKIP_CARGO_BUILD -ne "1") {
    Write-Host "Building WesoForge GUI (Tauri, no bundle)..." -ForegroundColor Cyan
    Push-Location (Join-Path $Root "crates/client-gui")
    try {
        cargo tauri build --no-bundle --features prod-backend
    }
    finally {
        Pop-Location
    }
}

$Version = Get-WorkspaceVersion
$Arch = Get-PlatformArch
$WebViewArch = Get-WebView2ArchFolder $Arch

$TargetDir = $env:CARGO_TARGET_DIR
if ([string]::IsNullOrWhiteSpace($TargetDir)) {
    $TargetDir = Join-Path $Root "target"
}

$ExeSrc = Join-Path $TargetDir "release\\bbr-client-gui.exe"
if (!(Test-Path -LiteralPath $ExeSrc)) {
    throw "Expected GUI binary not found at: $ExeSrc"
}
$ExeDst = Join-Path $AppDir "WesoForge.exe"
Copy-Item -Force -LiteralPath $ExeSrc -Destination $ExeDst

# MPIR runtime DLLs (required by chiavdf on Windows).
$MpirDir = Join-Path $Root "chiavdf\\mpir_gc_x64"
if (!(Test-Path -LiteralPath $MpirDir)) {
    throw "MPIR directory not found at: $MpirDir"
}
Copy-Item -Force -Path (Join-Path $MpirDir "mpir*.dll") -Destination $AppDir

# WebView2 loader DLL (required by wry/webview2 on Windows when dynamically linked).
$WebView2BuildRoot = Join-Path $TargetDir "release\\build"
$WebView2Loader = Get-ChildItem -LiteralPath $WebView2BuildRoot -Recurse -Filter "WebView2Loader.dll" |
    Where-Object { $_.FullName -match "\\out\\$WebViewArch\\" } |
    Select-Object -First 1
if ($null -eq $WebView2Loader) {
    throw "WebView2Loader.dll not found under $WebView2BuildRoot (arch folder: $WebViewArch)"
}
Copy-Item -Force -LiteralPath $WebView2Loader.FullName -Destination $AppDir

$ZipPath = Join-Path $DistDir ("WesoForge-gui_Windows_{0}_{1}.zip" -f $Version, $Arch)
if (Test-Path -LiteralPath $ZipPath) {
    Remove-Item -Force -LiteralPath $ZipPath
}
Compress-Archive -LiteralPath $AppDir -DestinationPath $ZipPath -Force

Write-Host "Wrote: $ZipPath" -ForegroundColor Green
