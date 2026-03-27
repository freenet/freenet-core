# Freenet installer for Windows
# Usage: irm https://freenet.org/install.ps1 | iex
#
# This script installs Freenet to %LOCALAPPDATA%\Freenet\bin\ and optionally
# sets up the system service (Task Scheduler + tray icon).
#
# Options (via environment variables):
#   FREENET_INSTALL_DIR  - Installation directory (default: %LOCALAPPDATA%\Freenet\bin)
#   FREENET_NO_SERVICE   - Set to 1 to skip service installation prompt
#   FREENET_VERSION      - Specific version to install (default: latest)

$ErrorActionPreference = "Stop"

# ── Helpers ──────────────────────────────────────────────────────────

function Write-Info($msg) { Write-Host "info: $msg" -ForegroundColor Blue }
function Write-Success($msg) { Write-Host "success: $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "warning: $msg" -ForegroundColor Yellow }
function Write-Err($msg) { Write-Host "error: $msg" -ForegroundColor Red; exit 1 }

# ── Platform check ───────────────────────────────────────────────────

if ([System.Environment]::Is64BitOperatingSystem -eq $false) {
    Write-Err "32-bit Windows is not supported. Please use a 64-bit system."
}

$arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
if ($arch -ne "X64") {
    Write-Err "Only x86_64 (AMD64) Windows is currently supported. Detected: $arch"
}

if ($PSVersionTable.PSVersion.Major -lt 5) {
    Write-Err "PowerShell 5.1 or later is required. Current: $($PSVersionTable.PSVersion)"
}

# ── Version resolution ───────────────────────────────────────────────

$target = "x86_64-pc-windows-msvc"
$githubApi = "https://api.github.com/repos/freenet/freenet-core/releases/latest"

if ($env:FREENET_VERSION) {
    $version = $env:FREENET_VERSION
    Write-Info "Installing specified version: $version"
} else {
    Write-Info "Fetching latest version..."
    try {
        $release = Invoke-RestMethod -Uri $githubApi -Headers @{ "User-Agent" = "freenet-installer" }
        $version = $release.tag_name -replace '^v', ''
    } catch {
        Write-Err "Failed to fetch latest version from GitHub: $_"
    }
    Write-Info "Latest version: $version"
}

# ── Install directory ────────────────────────────────────────────────

if ($env:FREENET_INSTALL_DIR) {
    $installDir = $env:FREENET_INSTALL_DIR
} else {
    $installDir = Join-Path $env:LOCALAPPDATA "Freenet\bin"
}

if (-not (Test-Path $installDir)) {
    Write-Info "Creating directory: $installDir"
    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
}

# ── Check for existing installation (upgrade) ────────────────────────

$existingFreenet = Join-Path $installDir "freenet.exe"
if (Test-Path $existingFreenet) {
    Write-Info "Existing installation found. Stopping Freenet if running..."
    try {
        & $existingFreenet service stop 2>$null
    } catch {
        # Ignore errors — may not be running
    }
    Start-Sleep -Seconds 2
}

# ── Download ─────────────────────────────────────────────────────────

$baseUrl = "https://github.com/freenet/freenet-core/releases/download/v$version"
$tempDir = Join-Path $env:TEMP "freenet-install-$(Get-Random)"
New-Item -ItemType Directory -Path $tempDir -Force | Out-Null

try {
    # Download checksums
    $checksumsAvailable = $false
    $checksumsPath = Join-Path $tempDir "SHA256SUMS.txt"
    try {
        Write-Info "Downloading checksums..."
        Invoke-WebRequest -Uri "$baseUrl/SHA256SUMS.txt" -OutFile $checksumsPath -UseBasicParsing
        $checksumsAvailable = $true
    } catch {
        Write-Warn "SHA256SUMS.txt not available for this release. Skipping checksum verification."
    }

    # Parse checksums into a hashtable
    $checksums = @{}
    if ($checksumsAvailable) {
        Get-Content $checksumsPath | ForEach-Object {
            $parts = $_ -split '\s+', 2
            if ($parts.Count -eq 2) {
                $checksums[$parts[1].Trim()] = $parts[0].Trim()
            }
        }
    }

    # Download freenet
    $freenetZip = "freenet-$target.zip"
    $freenetZipPath = Join-Path $tempDir $freenetZip
    Write-Info "Downloading freenet..."
    Invoke-WebRequest -Uri "$baseUrl/$freenetZip" -OutFile $freenetZipPath -UseBasicParsing

    if ($checksums.ContainsKey($freenetZip)) {
        Write-Info "Verifying freenet checksum..."
        $actual = (Get-FileHash -Path $freenetZipPath -Algorithm SHA256).Hash.ToLower()
        $expected = $checksums[$freenetZip].ToLower()
        if ($actual -ne $expected) {
            Write-Err "Checksum verification failed for $freenetZip`nExpected: $expected`nGot:      $actual"
        }
    }

    # Download fdev
    $fdevZip = "fdev-$target.zip"
    $fdevZipPath = Join-Path $tempDir $fdevZip
    try {
        Write-Info "Downloading fdev..."
        Invoke-WebRequest -Uri "$baseUrl/$fdevZip" -OutFile $fdevZipPath -UseBasicParsing

        if ($checksums.ContainsKey($fdevZip)) {
            Write-Info "Verifying fdev checksum..."
            $actual = (Get-FileHash -Path $fdevZipPath -Algorithm SHA256).Hash.ToLower()
            $expected = $checksums[$fdevZip].ToLower()
            if ($actual -ne $expected) {
                Write-Warn "Checksum verification failed for fdev. Skipping fdev installation."
                $fdevZipPath = $null
            }
        }
    } catch {
        Write-Warn "fdev not available for this release. Skipping."
        $fdevZipPath = $null
    }

    # ── Extract and install ──────────────────────────────────────────

    Write-Info "Installing to $installDir..."

    # Extract freenet
    $freenetExtract = Join-Path $tempDir "freenet-extract"
    Expand-Archive -Path $freenetZipPath -DestinationPath $freenetExtract -Force
    $freenetExe = Get-ChildItem -Path $freenetExtract -Filter "freenet.exe" -Recurse | Select-Object -First 1
    if (-not $freenetExe) {
        Write-Err "freenet.exe not found in archive"
    }
    Copy-Item -Path $freenetExe.FullName -Destination (Join-Path $installDir "freenet.exe") -Force

    # Extract fdev
    if ($fdevZipPath -and (Test-Path $fdevZipPath)) {
        $fdevExtract = Join-Path $tempDir "fdev-extract"
        Expand-Archive -Path $fdevZipPath -DestinationPath $fdevExtract -Force
        $fdevExe = Get-ChildItem -Path $fdevExtract -Filter "fdev.exe" -Recurse | Select-Object -First 1
        if ($fdevExe) {
            Copy-Item -Path $fdevExe.FullName -Destination (Join-Path $installDir "fdev.exe") -Force
        } else {
            Write-Warn "fdev.exe not found in archive. Skipping."
        }
    }

    # ── Verify installation ──────────────────────────────────────────

    $installedFreenet = Join-Path $installDir "freenet.exe"
    try {
        $versionOutput = & $installedFreenet --version 2>&1
        Write-Success "Installed: $versionOutput"
    } catch {
        Write-Err "Installation failed — freenet.exe does not execute correctly."
    }

    # ── Update PATH ──────────────────────────────────────────────────

    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($userPath -notlike "*$installDir*") {
        Write-Info "Adding $installDir to user PATH..."
        [Environment]::SetEnvironmentVariable("Path", "$userPath;$installDir", "User")
        # Also update current session
        $env:Path = "$env:Path;$installDir"
        Write-Success "Added to PATH. New terminal windows will have freenet available."
    } else {
        Write-Info "$installDir is already in PATH."
    }

    # ── Service installation ─────────────────────────────────────────

    if ($env:FREENET_NO_SERVICE -eq "1") {
        Write-Info "Skipping service installation (FREENET_NO_SERVICE=1)."
    } elseif ([Environment]::UserInteractive -and [Console]::In -ne $null) {
        # Interactive terminal — prompt the user
        Write-Host ""
        $installService = Read-Host "Install Freenet as a background service? (starts on login with tray icon) [Y/n]"
        if ($installService -eq "" -or $installService -match "^[Yy]") {
            Write-Info "Installing service..."
            & $installedFreenet service install
            if ($LASTEXITCODE -ne 0) {
                Write-Warn "Service installation failed (exit code $LASTEXITCODE). You may need to run as Administrator."
                Write-Warn "You can retry later with: freenet service install"
            }
        } else {
            Write-Info "Skipping service installation."
            Write-Info "You can install the service later with: freenet service install"
        }
    } else {
        # Non-interactive (piped via iex) — install service by default
        Write-Info "Installing service..."
        & $installedFreenet service install
        if ($LASTEXITCODE -ne 0) {
            Write-Warn "Service installation failed (exit code $LASTEXITCODE). You may need to run as Administrator."
            Write-Warn "You can retry later with: freenet service install"
        }
    }

    # ── Done ─────────────────────────────────────────────────────────

    Write-Host ""
    Write-Success "Freenet installation complete!"
    Write-Host ""
    Write-Host "To start Freenet now:"
    Write-Host "  freenet service start"
    Write-Host ""
    Write-Host "Or run manually:"
    Write-Host "  freenet network"
    Write-Host ""
    Write-Host "Dashboard: http://127.0.0.1:7509/"
    Write-Host ""

} finally {
    # Clean up temp directory
    if (Test-Path $tempDir) {
        Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
