# Changed from the original Chroma file; see the git history for the changes and their authors.
# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
# VecturaDB CLI Installer Script for Windows (PowerShell)
# Usage:
#   iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/rust/cli/install/install.ps1'))
# The command is vecturadb. To have it also as chroma, set $env:VECTURADB_CHROMA_ALIAS = "1" before running the script.

$ErrorActionPreference = 'Stop'

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if ($isAdmin) {
    $expectedInstallDir = Join-Path $env:ProgramFiles "VecturaDB"
} else {
    $expectedInstallDir = Join-Path $env:USERPROFILE "bin"
}
$expectedInstallPath = Join-Path $expectedInstallDir "vecturadb.exe"

$repo    = "VecturaDB/VecturaDB"

$release = @(Invoke-RestMethod -Uri "https://api.github.com/repos/$repo/releases?per_page=1" -Headers @{ "User-Agent" = "VecturaDB-installer" })[0]
$tag = $release.tag_name
if (-not $tag) { throw "No release found in $repo" }
$asset = "vecturadb-windows-x64.exe"
# Older releases have the binary as chroma-windows-x64.exe: then the script also installs it as chroma, the name
# those releases document.
if (-not ($release.assets | Where-Object { $_.name -eq $asset })) {
    $asset = "chroma-windows-x64.exe"
    $env:VECTURADB_CHROMA_ALIAS = "1"
}
$downloadUrl = "https://github.com/$repo/releases/download/$tag/$asset"
Write-Host "Downloading $asset from $downloadUrl ..."

$tempFile = Join-Path $env:TEMP "vecturadb.exe"
Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile

if (-not (Test-Path $expectedInstallDir)) {
    New-Item -ItemType Directory -Path $expectedInstallDir -Force | Out-Null
}

Move-Item -Path $tempFile -Destination $expectedInstallPath -Force

Write-Host "VecturaDB has been installed to: $expectedInstallPath"
if ($env:VECTURADB_CHROMA_ALIAS -eq "1") {
    $aliasPath = Join-Path $expectedInstallDir "chroma.exe"
    Copy-Item -Path $expectedInstallPath -Destination $aliasPath -Force
    Write-Host "Also installed as: $aliasPath"
}

$pathDirs = $env:PATH -split ';'
if ($pathDirs -notcontains $expectedInstallDir) {
    Write-Warning "WARNING ⚠️: The directory '$expectedInstallDir' is not in your PATH."
    Write-Host "To add it for the current session, run:"
    Write-Host "    `$env:PATH = '$expectedInstallDir;' + `$env:PATH"
    Write-Host "To add it permanently, update your system environment variables."
}
