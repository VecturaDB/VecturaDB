#!/usr/bin/env bash
# Changed by VecturaDB contributors for the VecturaDB branding.
# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
set -e

# ----------------------------------------------
# VecturaDB CLI Installer Script
# Usage:
#   curl -sSL https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/rust/cli/install/install.sh | bash
# ----------------------------------------------
REPO="VecturaDB/VecturaDB"

OS=$(uname -s)
ARCH=$(uname -m)
ASSET=""

case "$OS" in
  Linux*)
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
      ASSET="chroma-linux-aarch64"
    else
      ASSET="chroma-linux-x86_64"
    fi
    ;;
  Darwin*)
    if [ "$ARCH" = "arm64" ]; then
      ASSET="chroma-macos-arm64"
    else
      ASSET="chroma-macos-x64"
    fi
    ;;
  MINGW*|MSYS*|CYGWIN*)
    ASSET="chroma-windows-x64.exe"
    ;;
  *)
    echo "Unsupported OS: $OS"
    exit 1
    ;;
esac

TAG=$(curl -sSL "https://api.github.com/repos/${REPO}/releases?per_page=1" | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
if [ -z "$TAG" ]; then
  echo "No release found in ${REPO}"
  exit 1
fi
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${ASSET}"
echo "Downloading ${ASSET} from ${DOWNLOAD_URL}..."
curl -L "$DOWNLOAD_URL" -o chroma

chmod +x chroma

if [ -w /usr/local/bin ]; then
  sudo mv chroma /usr/local/bin/chroma
else
  mkdir -p "$HOME/.local/bin"
  mv chroma "$HOME/.local/bin/chroma"

  case ":$PATH:" in
    *:"$HOME/.local/bin":*)
      ;;
    *)
      echo "====================="
      echo "Warning ⚠️: $HOME/.local/bin is not in your PATH."
      echo "To add it, you can run:"
      echo '  export PATH="$HOME/.local/bin:$PATH"'
      echo "====================="
      ;;
  esac
fi
