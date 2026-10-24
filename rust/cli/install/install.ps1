# VecturaDB CLI Installer Script for Windows (PowerShell)
# Usage:
#   iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/rust/cli/install/install.ps1'))

$ErrorActionPreference = 'Stop'

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if ($isAdmin) {
    $expectedInstallDir = Join-Path $env:ProgramFiles "VecturaDB"
} else {
    $expectedInstallDir = Join-Path $env:USERPROFILE "bin"
}
$expectedInstallPath = Join-Path $expectedInstallDir "chroma.exe"

$repo    = "VecturaDB/VecturaDB"
$asset   = "chroma-windows-x64.exe"

$tag = @(Invoke-RestMethod -Uri "https://api.github.com/repos/$repo/releases?per_page=1" -Headers @{ "User-Agent" = "VecturaDB-installer" })[0].tag_name
if (-not $tag) { throw "No release found in $repo" }
$downloadUrl = "https://github.com/$repo/releases/download/$tag/$asset"
Write-Host "Downloading $asset from $downloadUrl ..."

$tempFile = Join-Path $env:TEMP "chroma.exe"
Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile

if (-not (Test-Path $expectedInstallDir)) {
    New-Item -ItemType Directory -Path $expectedInstallDir -Force | Out-Null
}

Move-Item -Path $tempFile -Destination $expectedInstallPath -Force

Write-Host "VecturaDB has been installed to: $expectedInstallPath"

$pathDirs = $env:PATH -split ';'
if ($pathDirs -notcontains $expectedInstallDir) {
    Write-Warning "WARNING ⚠️: The directory '$expectedInstallDir' is not in your PATH."
    Write-Host "To add it for the current session, run:"
    Write-Host "    `$env:PATH = '$expectedInstallDir;' + `$env:PATH"
    Write-Host "To add it permanently, update your system environment variables."
}
